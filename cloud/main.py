"""Cloud tier: Kafka consumer for edge telemetry + HTTP API + downlink publisher."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from confluent_kafka import Consumer, KafkaError, Producer

import core

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
EVENT_TOPIC = "twin-events"        # edge  -> cloud telemetry
ACK_TOPIC = "twin-acks"            # cloud -> edge  telemetry acks
CMD_TOPIC = "twin-commands"        # cloud -> edge  cloud-side writes
CMD_ACK_TOPIC = "twin-cmd-acks"    # edge  -> cloud command acks
DB_PATH = os.getenv("DB_PATH", "/data/cloud.db")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8080"))
BATCH = int(os.getenv("CLOUD_BATCH", "200"))

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)


# --------------------------------------------------------------------- HTTP
class Api(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        q = parse_qs(url.query)
        db = core.connect(DB_PATH)
        try:
            if url.path == "/health":
                self._send(200, {"ok": True})
            elif url.path == "/state":
                self._send(200, core.twin_state(db))
            elif url.path == "/event_ids":
                self._send(200, core.event_ids(db, (q.get("since") or [None])[0]))
            elif url.path == "/metrics":
                self._send(200, {
                    "events_total": db.execute(
                        "SELECT COUNT(*) FROM events").fetchone()[0],
                    "downlink_unacked": core.downlink_unacked(db)})
            else:
                self._send(404, {"error": "not found"})
        finally:
            db.close()

    def do_POST(self):
        if urlparse(self.path).path != "/write":
            return self._send(404, {"error": "not found"})
        db = core.connect(DB_PATH)
        try:
            n = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(n) or b"{}")
            self._send(200, core.local_write(
                db, body["twin_id"], body["property"], body["value"],
                body.get("event_time")))
        except Exception as exc:  # noqa: BLE001 - report any bad request
            self._send(400, {"error": str(exc)})
        finally:
            db.close()

    def log_message(self, *args):
        pass


def serve_http():
    ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Api).serve_forever()


# -------------------------------------------------------------------- Kafka
def make_clients():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": "cloud-twin-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 3000,   # see edge/sync.py
        "allow.auto.create.topics": True,
    })
    consumer.subscribe([EVENT_TOPIC, CMD_ACK_TOPIC])
    producer = Producer({"bootstrap.servers": KAFKA_SERVERS,
                         "client.id": "cloud-producer"})
    return consumer, producer


def run():
    db = core.connect(DB_PATH)
    consumer, producer = make_clients()
    last_downlink = 0.0
    print("Cloud running: Kafka consumer + HTTP API on "
          f":{HTTP_PORT}", flush=True)

    while True:
        msgs = consumer.consume(num_messages=BATCH, timeout=0.5)
        events, cmd_acks = [], []
        for m in msgs:
            if m.error():
                if m.error().code() not in (
                        KafkaError._PARTITION_EOF,
                        KafkaError.UNKNOWN_TOPIC_OR_PART):
                    print(f"Kafka error: {m.error()}", flush=True)
                continue
            if m.topic() == EVENT_TOPIC:
                events.append(json.loads(m.value().decode()))
            else:
                cmd_acks.append(json.loads(m.value().decode())["event_id"])

        if events:
            try:
                results = core.process_batch(db, events)  # ONE transaction
                for e in events:
                    producer.produce(ACK_TOPIC, key=e["event_id"],
                                     value=json.dumps({
                                         "event_id": e["event_id"],
                                         "cloud_committed_at": core.utc_now()}))
                if producer.flush(10):
                    raise RuntimeError("acks still queued")
                summary = {r: results.count(r) for r in set(results)}
                print(f"BATCH {len(events)} events {summary}", flush=True)
            except Exception as exc:  # leave offsets uncommitted -> redelivery
                print(f"Batch failed, will be redelivered: {exc}", flush=True)
                consumer.close()
                consumer, producer = make_clients()
                continue
        if cmd_acks:
            core.downlink_ack(db, cmd_acks)
            print(f"COMMAND ACKS {len(cmd_acks)}", flush=True)
        if msgs and (events or cmd_acks):
            consumer.commit(asynchronous=False)

        # Publish cloud-side writes the edge has not acknowledged yet.
        if time.time() - last_downlink >= 1:
            due = core.downlink_due(db)
            if due:
                for event_id, payload in due:
                    producer.produce(CMD_TOPIC, key=event_id, value=payload)
                producer.poll(0)
                core.downlink_mark_sent(db, [d[0] for d in due])
            last_downlink = time.time()


if __name__ == "__main__":
    threading.Thread(target=serve_http, daemon=True).start()
    while True:
        try:
            run()
        except Exception as exc:  # noqa: BLE001
            print(f"Cloud loop error: {exc}", flush=True)
            time.sleep(3)