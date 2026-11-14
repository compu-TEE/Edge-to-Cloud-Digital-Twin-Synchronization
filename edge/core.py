"""Edge-side storage logic: twin state, event history and the bounded outbox.

No network code lives here so the logic can be unit-tested and simulated
without MQTT/Kafka (see tests/).

Outbox capacity and eviction policy ("latest-protected drop-oldest")
--------------------------------------------------------------------
The outbox holds events that still have to reach the cloud (status 'pending' or
'sent').  Its size is bounded by MAX_OUTBOX_EVENTS.  When a new edge event
arrives and the outbox is full:

 1. The newest accepted value of every (twin_id, property) -- the one recorded
    in property_versions -- is PROTECTED and is never evicted.
 2. Otherwise the OLDEST queued event is evicted (status -> 'evicted',
    counter evicted_events++).  Evicted events stay in the local history but
    are never sent; they are the only events that can be lost.
 3. If nothing is evictable (capacity <= number of live properties) the NEW
    event is rejected atomically: neither the twin state nor the version table
    is touched, so edge and cloud cannot diverge, and dropped_events++.

Rationale: for telemetry the newest reading per property supersedes older
ones, so under overflow we sacrifice old history (cheap) and never the current
state of the twin.  Eviction therefore degrades history, not convergence.
"""
import json
import sqlite3
from datetime import datetime, timezone

from conflict import parse_ts, wins

UNACKED = "('pending','sent')"

SCHEMA = """
CREATE TABLE IF NOT EXISTS twins (
    twin_id TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    twin_id TEXT NOT NULL,
    property TEXT NOT NULL,
    value_json TEXT NOT NULL,
    event_time TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    sent_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, created_at);
CREATE TABLE IF NOT EXISTS metrics (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS property_versions (
    twin_id TEXT NOT NULL,
    property TEXT NOT NULL,
    event_time TEXT NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY (twin_id, property)
);
INSERT OR IGNORE INTO metrics(name, value) VALUES ('dropped_events', 0);
INSERT OR IGNORE INTO metrics(name, value) VALUES ('evicted_events', 0);
"""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def connect(path):
    """Open the edge DB (autocommit; transactions are explicit)."""
    db = sqlite3.connect(path, timeout=10, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA busy_timeout=10000")
    db.executescript(SCHEMA)
    return db


def inc_metric(db, name, amount=1):
    db.execute(
        "INSERT INTO metrics(name, value) VALUES (?, ?) "
        "ON CONFLICT(name) DO UPDATE SET value = value + excluded.value",
        (name, amount),
    )


def unacked_count(db):
    return db.execute(
        f"SELECT COUNT(*) FROM outbox WHERE status IN {UNACKED}"
    ).fetchone()[0]


def _pick_victim(db):
    """Oldest queued event that is not the current winner of its property."""
    row = db.execute(f"""
        SELECT o.event_id
        FROM outbox o
        JOIN events e ON e.event_id = o.event_id
        LEFT JOIN property_versions pv
               ON pv.twin_id = e.twin_id
              AND pv.property = e.property
              AND pv.event_id = o.event_id
        WHERE o.status IN {UNACKED} AND pv.event_id IS NULL
        ORDER BY o.created_at, o.rowid
        LIMIT 1
    """).fetchone()
    return row[0] if row else None


def apply_event(db, event, max_outbox, origin="edge", now=None):
    """Record one event.  Returns a dict: outcome and optional evicted id.

    outcome: 'applied' | 'stale' | 'duplicate' | 'dropped'
    origin : 'edge'  -> sensor reading; queued in the outbox for the cloud
             'cloud' -> command received from the cloud; recorded locally only
    """
    required = {"event_id", "twin_id", "property", "value", "event_time"}
    missing = required - set(event)
    if missing:
        raise ValueError(f"Missing fields: {sorted(missing)}")

    event_id = str(event["event_id"])
    twin_id = str(event["twin_id"])
    prop = str(event["property"])
    event_time = str(event["event_time"])
    parse_ts(event_time)  # reject malformed timestamps early
    received_at = now or utc_now()

    db.execute("BEGIN IMMEDIATE")
    try:
        if db.execute(
            "SELECT 1 FROM events WHERE event_id = ?", (event_id,)
        ).fetchone():
            db.execute("ROLLBACK")
            return {"outcome": "duplicate", "evicted": None}

        cur = db.execute(
            "SELECT event_time, event_id FROM property_versions "
            "WHERE twin_id = ? AND property = ?", (twin_id, prop)
        ).fetchone()
        won = wins(event_time, event_id,
                   cur[0] if cur else None, cur[1] if cur else None)

        if won:
            # Version first: the previous winner becomes evictable below.
            db.execute("""
                INSERT INTO property_versions(twin_id, property, event_time, event_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(twin_id, property) DO UPDATE SET
                    event_time = excluded.event_time, event_id = excluded.event_id
            """, (twin_id, prop, event_time, event_id))

        evicted = None
        if origin == "edge" and unacked_count(db) >= max_outbox:
            evicted = _pick_victim(db)
            if evicted is None:
                # Nothing safe to evict: reject atomically (no state change).
                db.execute("ROLLBACK")
                inc_metric(db, "dropped_events")
                return {"outcome": "dropped", "evicted": None}
            db.execute(
                "UPDATE outbox SET status = 'evicted' WHERE event_id = ?",
                (evicted,),
            )
            inc_metric(db, "evicted_events")

        if won:
            row = db.execute(
                "SELECT state_json FROM twins WHERE twin_id = ?", (twin_id,)
            ).fetchone()
            state = json.loads(row[0]) if row else {}
            state[prop] = event["value"]
            db.execute("""
                INSERT INTO twins(twin_id, state_json, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(twin_id) DO UPDATE SET
                    state_json = excluded.state_json, updated_at = excluded.updated_at
            """, (twin_id, json.dumps(state), received_at))

        db.execute(
            "INSERT INTO events(event_id, twin_id, property, value_json, "
            "event_time, received_at) VALUES (?, ?, ?, ?, ?, ?)",
            (event_id, twin_id, prop, json.dumps(event["value"]),
             event_time, received_at),
        )
        if origin == "edge":
            db.execute(
                "INSERT INTO outbox(event_id, payload_json, status, created_at) "
                "VALUES (?, ?, 'pending', ?)",
                (event_id, json.dumps(event), received_at),
            )
        db.execute("COMMIT")
        return {"outcome": "applied" if won else "stale", "evicted": evicted}
    except Exception:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise


def mark_acked(db, event_ids):
    if not event_ids:
        return
    db.execute("BEGIN IMMEDIATE")
    db.executemany(
        "UPDATE outbox SET status = 'acked' WHERE event_id = ? "
        "AND status != 'evicted'",
        [(e,) for e in event_ids],
    )
    db.execute("COMMIT")


def prune(db, keep=2000):
    """Bound disk use: keep only the newest `keep` finished outbox rows."""
    db.execute("BEGIN IMMEDIATE")
    db.execute("""
        DELETE FROM outbox
        WHERE status IN ('acked','evicted')
          AND rowid NOT IN (
              SELECT rowid FROM outbox
              WHERE status IN ('acked','evicted')
              ORDER BY rowid DESC LIMIT ?)
    """, (keep,))
    db.execute("COMMIT")


def metrics(db, capacity=None):
    out = dict(db.execute("SELECT name, value FROM metrics").fetchall())
    counts = dict(db.execute(
        "SELECT status, COUNT(*) FROM outbox GROUP BY status").fetchall())
    out["outbox_unacked"] = counts.get("pending", 0) + counts.get("sent", 0)
    out["outbox_pending"] = counts.get("pending", 0)
    out["outbox_sent"] = counts.get("sent", 0)
    out["events_total"] = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    out["capacity"] = capacity
    return out


def twin_state(db):
    return {t: json.loads(s) for t, s in
            db.execute("SELECT twin_id, state_json FROM twins")}


def event_ids(db, since=None):
    if since:
        rows = db.execute(
            "SELECT event_id FROM events WHERE received_at >= ?", (since,))
    else:
        rows = db.execute("SELECT event_id FROM events")
    return [r[0] for r in rows]
