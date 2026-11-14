# Resilient Edge–Cloud Synchronisation

Two-tier system (edge + cloud) that keeps working through a network outage, buffers
locally under a **bounded, documented eviction policy**, and reconciles on reconnect —
including when **both tiers changed the same value while disconnected**.

| Requirement | Implementation |
|---|---|
| Edge messaging | MQTT (Mosquitto), QoS 1 |
| Central messaging | Kafka (`twin-events`, `twin-acks`, `twin-commands`, `twin-cmd-acks`) |
| Local buffer | SQLite outbox (WAL, `synchronous=FULL`), bounded by `MAX_OUTBOX_EVENTS` |
| Network simulation | custom TCP proxy, `OFF`/`ON`/`STATUS` on port 9095 |
| Resource limits | edge + edge-sync containers: `cpus: 1.0`, `mem_limit: 256m` |

## Policies (defended in `report/report.pdf`)
* **Buffer – latest-protected drop-oldest** (`edge/core.py`): when full, evict the oldest queued
  event *except* the newest value of each (twin, property); if nothing is evictable, reject the new
  event atomically. Overflow loses old history, never current state. Counters: `evicted_events`, `dropped_events`.
* **Conflicts – per-property last-writer-wins** on `(event_time, event_id)` (`conflict.py`, identical
  copy in `edge/` and `cloud/`, enforced by a test). Total order ⇒ both tiers converge for any delivery order.

## Run
```bash
docker compose up -d --build            # everything
python -m unittest discover -s tests -v # 17 logic tests, no Docker needed
python scripts/sim_outage.py            # logic-level outage simulation (real storage code, virtual link)
python scripts/run_experiments.py       # REAL outages: short/medium/long x3 repeats + 3 conflict scenarios
python scripts/build_report.py          # rebuild every table/figure and report/report.pdf (needs pdflatex)
```
`MAX_OUTBOX_EVENTS` and `SENSOR_INTERVAL` are set in `.env`. The long outage must exceed
capacity ÷ ingest rate (~210 s for 1000 events at ~4.8 events/s): `run_experiments.py --long 300`.

Manual outage:
```bash
printf 'OFF\n' | nc -w 2 localhost 9095; sleep 60; printf 'ON\n' | nc -w 2 localhost 9095
```
Status APIs: edge `http://localhost:8081/{metrics,state,event_ids}`, cloud `http://localhost:8080/{health,state,metrics,event_ids}`.
Write at the cloud (to try a two-sided conflict by hand while the proxy is OFF):
```bash
curl -X POST localhost:8080/write -d '{"twin_id":"motor-01","property":"setpoint","value":70}'
```

## Layout
`edge/` receiver · sync worker · `core.py` (buffer/eviction) · sensor — `cloud/` consumer · HTTP API · `core.py` —
`proxy/` fault injector — `scripts/` experiments, simulation, report builder — `tests/` — `results/` — `report/`

## Status / honesty notes
* Unit tests and the outage simulation exercise the real storage and policy code.
* `edge/sync.py`, `cloud/main.py` and `scripts/run_experiments.py` (the Kafka/Docker glue) compile but must be
  exercised with `docker compose up` — run `run_experiments.py` once and confirm before submitting.
* Simulation timings use an **assumed** drain rate; only `run_experiments.py` output is a real measurement.
* Start from a clean `data/` directory (the schema gained tables/columns): `rm -rf data/*/` then `docker compose up`.
