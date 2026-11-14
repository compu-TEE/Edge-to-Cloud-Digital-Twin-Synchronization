"""Logic-level outage simulation (no Docker/Kafka needed).

Drives the REAL edge/core.py and cloud/core.py (real SQLite, real eviction and
LWW code) through a virtual link with a virtual clock.  It validates the
buffer policy, data-loss accounting and convergence.  It does NOT measure
network or Kafka timings: the drain rate is an explicit assumption
(--drain-rate, events/s) -- real latencies come from scripts/run_experiments.py.

Usage: python scripts/sim_outage.py [--out results] [--ingest 5] [--drain-rate 18]
"""
import argparse
import csv
import importlib.util
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "edge"))
import core as edge  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "cloud_core", os.path.join(ROOT, "cloud", "core.py"))
cloud = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cloud)

T0 = datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc)
PROPS = [("motor-01", "temperature"), ("motor-01", "vibration"),
         ("pump-01", "pressure"), ("pump-01", "flow_rate"),
         ("tank-01", "level"), ("tank-01", "temperature")]


def run(outage_s, capacity, ingest, drain, warm=20, tail_limit=3600):
    with tempfile.TemporaryDirectory() as d:
        e = edge.connect(os.path.join(d, "edge.db"))
        c = cloud.connect(os.path.join(d, "cloud.db"))
        t, n, link_up = 0, 0, True
        peak, t_on, drained_at, series = 0, None, None, []
        ingest_stop = warm + outage_s + 5   # sensors stop shortly after recovery

        def ingest_tick(sec):
            nonlocal n
            for _ in range(ingest):
                twin, prop = PROPS[n % len(PROPS)]
                ts = (T0 + timedelta(seconds=sec + _ / ingest)).isoformat()
                edge.apply_event(e, {"event_id": f"ev{n:07d}", "twin_id": twin,
                                     "property": prop, "value": n,
                                     "event_time": ts}, capacity,
                                 now=ts)
                n += 1

        def drain_tick():
            rows = e.execute(
                "SELECT event_id, payload_json FROM outbox WHERE status='pending'"
                " ORDER BY created_at, rowid LIMIT ?", (drain,)).fetchall()
            if rows:
                cloud.process_batch(c, [json.loads(p) for _, p in rows])
                edge.mark_acked(e, [r[0] for r in rows])

        while True:
            if t == warm:
                link_up = False
            if t == warm + outage_s:
                link_up, t_on = True, t
            if t < ingest_stop:
                ingest_tick(t)
            if link_up:
                drain_tick()
            q = edge.unacked_count(e)
            peak = max(peak, q)
            series.append((t, q))
            if t_on is not None and drained_at is None and q <= 20:
                drained_at = t  # first sample back at (near) baseline
            if (t_on is not None and t >= ingest_stop and q == 0) or t > tail_limit:
                break
            t += 1

        m = edge.metrics(e, capacity)
        edge_ids = set(edge.event_ids(e))
        cloud_ids = set(cloud.event_ids(c))
        lost = len(edge_ids - cloud_ids)
        generated = m["events_total"] + m["dropped_events"]
        res = {
            "outage_s": outage_s, "capacity": capacity,
            "generated": generated,
            "peak_backlog": peak,
            "buffer_filled": peak >= capacity,
            "evicted": m["evicted_events"], "rejected": m["dropped_events"],
            "lost_events": lost + m["dropped_events"],
            "loss_pct": round(100 * (lost + m["dropped_events"]) / generated, 2),
            "drain_s_after_reconnect": (drained_at - t_on),
            "twin_state_converged": edge.twin_state(e) == cloud.twin_state(c),
            "history_missing_in_cloud": lost,
            "cloud_only_ids": len(cloud_ids - edge_ids),
        }
        e.close(); c.close()
        return res, series


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(ROOT, "results"))
    ap.add_argument("--ingest", type=int, default=5, help="events/s at the edge")
    ap.add_argument("--drain-rate", type=int, default=18,
                    help="ASSUMED edge->cloud throughput after reconnect, events/s")
    ap.add_argument("--capacity", type=int, default=1000)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    rows, allseries = [], {}
    for name, dur in [("short", 10), ("medium", 60), ("long", 300)]:
        r, s = run(dur, a.capacity, a.ingest, a.drain_rate)
        r["name"] = name
        rows.append(r); allseries[name] = s
        print(name, r)
    # tight-buffer stress: capacity far below the outage's event volume
    r, s = run(60, 100, a.ingest, a.drain_rate)
    r["name"] = "stress(cap=100)"; rows.append(r); allseries["stress"] = s
    print("stress", r)
    keys = ["name", "outage_s", "capacity", "generated", "peak_backlog",
            "buffer_filled", "evicted", "rejected", "lost_events", "loss_pct",
            "drain_s_after_reconnect", "twin_state_converged",
            "history_missing_in_cloud", "cloud_only_ids"]
    with open(os.path.join(a.out, "sim_outages.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
        for r in rows: w.writerow({k: r[k] for k in keys})
    with open(os.path.join(a.out, "sim_backlog.json"), "w") as f:
        json.dump({"params": vars(a), "series": allseries}, f)


if __name__ == "__main__":
    main()
