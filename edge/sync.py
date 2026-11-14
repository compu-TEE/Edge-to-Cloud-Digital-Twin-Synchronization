"""Edge sync worker: outbox -> Kafka, acks and cloud commands <- Kafka.

All Kafka traffic goes through the fault-injection proxy (proxy:9094), so an
outage blocks uplink, acks and downlink alike.  librdkafka reconnects on its
own once the proxy is switched back on.
"""
import json
import os
import time

from confluent_kafka import Consumer, KafkaError, Producer

import core

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
EVENT_TOPIC = "twin-events"
ACK_TOPIC = "twin-acks"
CMD_TOPIC = "twin-commands"
CMD_ACK_TOPIC = "twin-cmd-acks"
DB_PATH = os.getenv("DB_PATH", "/data/edge.db")
MAX_OUTBOX_EVENTS = max(1, int(os.getenv("MAX_OUTBOX_EVENTS", "1000")))
RETRY_SECONDS = 5
SEND_BATCH = 500
PRUNE_KEEP = int(os.getenv("PRUNE_KEEP", "2000"))


def main():
    while not os.path.exists(DB_PATH):
        print("Waiting for edge SQLite database...", flush=True)
        time.sleep(1)
    db = core.connect(DB_PATH)

    def on_delivery(err, msg):
        # Not accepted by the broker (outage): make it eligible for retry.
        if err is not None:
            db.execute("UPDATE outbox SET status='pending' "
                       "WHERE event_id=? AND status='sent'",
                       (msg.key().decode(),))

    producer = Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "client.id": "edge-sync-producer",
        "message.timeout.ms": 10000,
    })
    consumer = Consumer({
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": "edge-ack-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([ACK_TOPIC, CMD_TOPIC])
    print("Edge sync started.", flush=True)

    last_send = last_prune = last_log = 0.0
    while True:
        msgs = consumer.consume(num_messages=200, timeout=0.2)
        acks, handled = [], False
        for m in msgs:
            if m.error():
                if m.error().code() not in (
                        KafkaError._PARTITION_EOF,
                        KafkaError.UNKNOWN_TOPIC_OR_PART,
                        KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN):
                    print(f"Kafka error: {m.error()}", flush=True)
                continue
            handled = True
            body = json.loads(m.value().decode())
            if m.topic() == ACK_TOPIC:
                acks.append(body["event_id"])
            else:  # cloud-originated write: same LWW rule, no outbox entry
                res = core.apply_event(db, body, MAX_OUTBOX_EVENTS, origin="cloud")
                print(f"CLOUD CMD {res['outcome'].upper()} "
                      f"{body['twin_id']}/{body['property']}", flush=True)
                producer.produce(CMD_ACK_TOPIC, key=body["event_id"],
                                 value=json.dumps({"event_id": body["event_id"]}))
        if acks:
            core.mark_acked(db, acks)
        if handled:
            producer.flush(5)
            consumer.commit(asynchronous=False)

        now = time.time()
        if now - last_send >= 0.5:
            rows = db.execute("""
                SELECT event_id, payload_json FROM outbox
                WHERE status = 'pending'
                   OR (status = 'sent' AND
                       (julianday('now') - julianday(sent_at)) * 86400.0 > ?)
                ORDER BY created_at, rowid LIMIT ?
            """, (RETRY_SECONDS, SEND_BATCH)).fetchall()
            if rows:
                ts = core.utc_now()
                db.execute("BEGIN IMMEDIATE")
                for event_id, payload in rows:
                    try:
                        producer.produce(EVENT_TOPIC, key=event_id, value=payload,
                                         on_delivery=on_delivery)
                    except BufferError:
                        break
                    db.execute("UPDATE outbox SET status='sent', sent_at=? "
                               "WHERE event_id=?", (ts, event_id))
                db.execute("COMMIT")
            producer.poll(0)
            last_send = now

        if now - last_prune >= 30:
            core.prune(db, PRUNE_KEEP)
            last_prune = now
        if now - last_log >= 15:
            print(f"Unacked outbox events: {core.unacked_count(db)}", flush=True)
            last_log = now


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as exc:  # noqa: BLE001
            print(f"Edge sync error: {exc}", flush=True)
            time.sleep(3)
