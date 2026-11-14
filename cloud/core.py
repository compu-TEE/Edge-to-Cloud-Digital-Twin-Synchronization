"""Cloud-side storage logic (no network code; unit-testable).

The cloud is a full peer: it consumes edge telemetry AND accepts its own writes
(operator commands / setpoints) while the edge is unreachable.  Both kinds of
event go through the same LWW rule (conflict.wins), so a value changed on both
tiers during an outage converges identically on both sides after reconnect.

Cloud-originated writes are stored in `downlink` and published to the edge
until the edge acknowledges them (at-least-once; the edge de-duplicates).
"""
import json
import sqlite3
import uuid
from datetime import datetime, timezone

from conflict import parse_ts, wins

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
CREATE TABLE IF NOT EXISTS property_versions (
    twin_id TEXT NOT NULL,
    property TEXT NOT NULL,
    event_time TEXT NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY (twin_id, property)
);
CREATE TABLE IF NOT EXISTS downlink (
    event_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    sent_at TEXT
);
"""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def connect(path):
    db = sqlite3.connect(path, timeout=10, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA busy_timeout=10000")
    db.executescript(SCHEMA)
    return db


def _apply(db, event, now):
    """Apply one event inside an already-open transaction."""
    required = {"event_id", "twin_id", "property", "value", "event_time"}
    missing = required - set(event)
    if missing:
        raise ValueError(f"Missing fields: {sorted(missing)}")
    event_id = str(event["event_id"])
    twin_id = str(event["twin_id"])
    prop = str(event["property"])
    event_time = str(event["event_time"])
    parse_ts(event_time)

    if db.execute("SELECT 1 FROM events WHERE event_id = ?",
                  (event_id,)).fetchone():
        return "duplicate"

    db.execute(
        "INSERT INTO events(event_id, twin_id, property, value_json, "
        "event_time, received_at) VALUES (?, ?, ?, ?, ?, ?)",
        (event_id, twin_id, prop, json.dumps(event["value"]),
         event_time, now),
    )
    cur = db.execute(
        "SELECT event_time, event_id FROM property_versions "
        "WHERE twin_id = ? AND property = ?", (twin_id, prop)).fetchone()
    won = wins(event_time, event_id,
               cur[0] if cur else None, cur[1] if cur else None)
    if won:
        row = db.execute("SELECT state_json FROM twins WHERE twin_id = ?",
                         (twin_id,)).fetchone()
        state = json.loads(row[0]) if row else {}
        state[prop] = event["value"]
        db.execute("""
            INSERT INTO twins(twin_id, state_json, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(twin_id) DO UPDATE SET
                state_json = excluded.state_json, updated_at = excluded.updated_at
        """, (twin_id, json.dumps(state), now))
        db.execute("""
            INSERT INTO property_versions(twin_id, property, event_time, event_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(twin_id, property) DO UPDATE SET
                event_time = excluded.event_time, event_id = excluded.event_id
        """, (twin_id, prop, event_time, event_id))
    return "applied" if won else "stale"


def process_batch(db, events):
    """Apply many edge events in ONE transaction (one fsync, not one per event).

    All-or-nothing: if any event is invalid the whole batch rolls back and the
    caller must not commit Kafka offsets, so the batch is re-delivered.
    Returns the list of per-event results in order.
    """
    now = utc_now()
    db.execute("BEGIN IMMEDIATE")
    try:
        results = [_apply(db, e, now) for e in events]
        db.execute("COMMIT")
        return results
    except Exception:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise


def local_write(db, twin_id, prop, value, event_time=None):
    """A write made directly at the cloud (also queued for the edge)."""
    event = {
        "event_id": f"cloud-{uuid.uuid4()}",
        "twin_id": twin_id,
        "property": prop,
        "value": value,
        "event_time": event_time or utc_now(),
        "origin": "cloud",
    }
    now = utc_now()
    db.execute("BEGIN IMMEDIATE")
    try:
        result = _apply(db, event, now)
        db.execute(
            "INSERT INTO downlink(event_id, payload_json, status, created_at) "
            "VALUES (?, ?, 'pending', ?)",
            (event["event_id"], json.dumps(event), now),
        )
        db.execute("COMMIT")
    except Exception:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise
    return {"event": event, "result": result}


def downlink_due(db, retry_seconds=5, limit=200):
    return db.execute("""
        SELECT event_id, payload_json FROM downlink
        WHERE status = 'pending'
           OR (status = 'sent'
               AND (julianday('now') - julianday(sent_at)) * 86400.0 > ?)
        ORDER BY created_at LIMIT ?
    """, (retry_seconds, limit)).fetchall()


def downlink_mark_sent(db, event_ids):
    db.execute("BEGIN IMMEDIATE")
    db.executemany(
        "UPDATE downlink SET status='sent', sent_at=? WHERE event_id=? "
        "AND status != 'acked'", [(utc_now(), e) for e in event_ids])
    db.execute("COMMIT")


def downlink_ack(db, event_ids):
    db.execute("BEGIN IMMEDIATE")
    db.executemany("UPDATE downlink SET status='acked' WHERE event_id=?",
                   [(e,) for e in event_ids])
    db.execute("COMMIT")


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


def downlink_unacked(db):
    return db.execute(
        "SELECT COUNT(*) FROM downlink WHERE status != 'acked'").fetchone()[0]
