#!/usr/bin/env python3
"""Run the real outage + conflict experiments against the Docker Compose stack.

Prerequisites:  docker compose up -d --build   (all services healthy)
Run:            python scripts/run_experiments.py [--short 10 --medium 60 --long 300 --repeats 3]

Needs only the Python standard library (+ matplotlib for the plot).
Outputs (results/): outages.csv, backlog_<run>.csv, conflicts.csv,
summary.json, backlog.png, docker_stats.txt

Definitions
  peak backlog      max unacked outbox rows sampled every 0.5 s during the outage
  reconciliation    seconds from proxy ON until backlog <= --drain-threshold for
  latency           two consecutive samples (sensors keep running)
  data loss         edge events (received during the run window) missing from the
                    cloud once drained + events rejected at the edge
  conflict          seconds from proxy ON until edge and cloud agree on the value
  convergence
"""
import argparse
import csv
import json
import os
import socket
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

EDGE = "http://localhost:8081"
CLOUD = "http://localhost:8080"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def http(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def proxy(cmd):
    with socket.create_connection(("localhost", 9095), timeout=5) as s:
        s.sendall((cmd + "\n").encode())
        return s.recv(200).decode()


def sh(*args):
    return subprocess.run(args, cwd=ROOT, check=True, capture_output=True, text=True).stdout


def now():
    return datetime.now(timezone.utc)


def unacked():
    return http("GET", f"{EDGE}/metrics")["outbox_unacked"]


def wait_until(pred, timeout, step=0.5):
    t = time.time()
    while time.time() - t < timeout:
        if pred():
            return time.time() - t
        time.sleep(step)
    return None


def run_outage(name, dur, thresh, series_dir):
    sh("docker", "compose", "start", "sensor-simulator")
    proxy("ON")
    time.sleep(5)
    m0 = http("GET", f"{EDGE}/metrics")
    cap = m0["capacity"]
    t0 = now().isoformat()
    series = []
    print(f"[{name}] outage {dur}s ...", flush=True)
    proxy("OFF")
    t_off = time.time()
    peak = 0
    while time.time() - t_off < dur:
        q = unacked(); peak = max(peak, q)
        series.append((round(time.time() - t_off, 2), q, "down"))
        time.sleep(0.5)
    proxy("ON")
    t_on = time.time()
    ok, latency = 0, None
    while time.time() - t_on < 900:
        q = unacked(); peak = max(peak, q)
        series.append((round(time.time() - t_off, 2), q, "up"))
        ok = ok + 1 if q <= thresh else 0
        if ok >= 2:
            latency = round(time.time() - t_on - 0.5, 1)  # first of the two samples
            break
        time.sleep(0.5)
    sh("docker", "compose", "stop", "sensor-simulator")      # quiesce for exact accounting
    wait_until(lambda: unacked() == 0, 600)
    time.sleep(2)
    m1 = http("GET", f"{EDGE}/metrics")
    q = urllib.parse.quote(t0)
    edge_ids = set(http("GET", f"{EDGE}/event_ids?since={q}", timeout=60))
    cloud_ids = set(http("GET", f"{CLOUD}/event_ids?since={q}", timeout=60))
    missing = len(edge_ids - cloud_ids)
    rejected = m1["dropped_events"] - m0["dropped_events"]
    generated = len(edge_ids) + rejected
    converged = http("GET", f"{EDGE}/state") == http("GET", f"{CLOUD}/state")
    res = {
        "run": name, "outage_s": dur, "capacity": cap, "generated": generated,
        "peak_backlog": peak, "buffer_filled": peak >= cap,
        "evicted": m1["evicted_events"] - m0["evicted_events"],
        "rejected": rejected, "lost_events": missing + rejected,
        "loss_pct": round(100 * (missing + rejected) / max(generated, 1), 2),
        "reconcile_latency_s": latency, "twin_state_converged": converged,
        "cloud_only_ids": len(cloud_ids - edge_ids),
    }
    with open(os.path.join(series_dir, f"backlog_{name}.csv"), "w", newline="") as f:
        w = csv.writer(f); w.writerow(["t_s", "unacked", "link"]); w.writerows(series)
    print(f"[{name}] {res}", flush=True)
    return res


def mqtt_edge_write(prop, value, when, eid):
    ev = {"event_id": eid, "twin_id": "motor-01", "property": prop,
          "value": value, "event_time": when.isoformat()}
    sh("docker", "exec", "cc-mp-mosquitto", "mosquitto_pub", "-t",
       "factory/motor-01/telemetry", "-q", "1", "-m", json.dumps(ev))


def run_conflict(scn, edge_off_s, cloud_off_s, expect):
    """Both tiers write the same property while disconnected."""
    prop = f"setpoint_{scn}_{int(time.time())}"
    base = now()
    proxy("OFF")
    time.sleep(1)
    mqtt_edge_write(prop, "EDGE", base + timedelta(seconds=edge_off_s),
                    f"conf-{prop}-edge")
    http("POST", f"{CLOUD}/write", {
        "twin_id": "motor-01", "property": prop, "value": "CLOUD",
        "event_time": (base + timedelta(seconds=cloud_off_s)).isoformat()})
    time.sleep(2)
    # During the outage each tier must keep its own value (independence).
    during = (http("GET", f"{EDGE}/state").get("motor-01", {}).get(prop),
              http("GET", f"{CLOUD}/state").get("motor-01", {}).get(prop))
    proxy("ON")
    t_on = time.time()

    def agree():
        e = http("GET", f"{EDGE}/state").get("motor-01", {}).get(prop)
        c = http("GET", f"{CLOUD}/state").get("motor-01", {}).get(prop)
        return e == c and e is not None
    lat = wait_until(agree, 180)
    e = http("GET", f"{EDGE}/state").get("motor-01", {}).get(prop)
    c = http("GET", f"{CLOUD}/state").get("motor-01", {}).get(prop)
    res = {"scenario": scn, "during_outage_edge": during[0],
           "during_outage_cloud": during[1], "final_edge": e, "final_cloud": c,
           "expected": expect, "converged": e == c,
           "correct_winner": (e == expect) if expect else None,
           "convergence_latency_s": None if lat is None else round(lat, 1)}
    print(f"[conflict:{scn}] {res}", flush=True)
    return res


def plot(results_dir, runs):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return
    fig, ax = plt.subplots(figsize=(7, 3.6))
    for r in runs:
        with open(os.path.join(results_dir, f"backlog_{r['run']}.csv")) as f:
            rows = list(csv.DictReader(f))
        ax.plot([float(x["t_s"]) for x in rows], [int(x["unacked"]) for x in rows],
                label=r["run"], lw=1)
    cap = runs[0]["capacity"]
    ax.axhline(cap, color="k", ls="--", lw=0.8, label=f"capacity={cap}")
    ax.set_xlabel("time since link cut (s)"); ax.set_ylabel("unacked outbox events")
    ax.legend(fontsize=7); ax.grid(alpha=.3); fig.tight_layout()
    fig.savefig(os.path.join(results_dir, "backlog.png"), dpi=200)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--short", type=int, default=10)
    ap.add_argument("--medium", type=int, default=60)
    ap.add_argument("--long", type=int, default=300,
                    help="must exceed capacity/ingest-rate (~210 s for 1000 @ ~4.8 ev/s)")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--drain-threshold", type=int, default=20)
    a = ap.parse_args()
    out = os.path.join(ROOT, "results")
    os.makedirs(out, exist_ok=True)

    assert http("GET", f"{CLOUD}/health")["ok"], "cloud API not reachable"
    runs = []
    for rep in range(1, a.repeats + 1):
        for label, dur in (("short", a.short), ("medium", a.medium), ("long", a.long)):
            runs.append(run_outage(f"{label}-{rep}", dur, a.drain_threshold, out))
    keys = list(runs[0])
    with open(os.path.join(out, "outages.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys); w.writeheader(); w.writerows(runs)

    conflicts = [
        run_conflict("cloud_newer", 0, 5, "CLOUD"),
        run_conflict("edge_newer", 5, 0, "EDGE"),
        run_conflict("same_timestamp", 3, 3, None),
    ]
    with open(os.path.join(out, "conflicts.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(conflicts[0])); w.writeheader()
        w.writerows(conflicts)

    stats = sh("docker", "stats", "--no-stream", "--format",
               "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}")
    open(os.path.join(out, "docker_stats.txt"), "w").write(stats)
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump({"outages": runs, "conflicts": conflicts,
                   "generated_at": now().isoformat()}, f, indent=2)
    plot(out, runs)
    sh("docker", "compose", "start", "sensor-simulator")
    print("Done. See results/")


if __name__ == "__main__":
    main()
