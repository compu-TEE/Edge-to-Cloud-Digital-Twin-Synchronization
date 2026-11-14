"""Edge tier: MQTT receiver. Storage/eviction logic lives in core.py."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import paho.mqtt.client as mqtt

import core

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
DB_PATH = os.getenv("DB_PATH", "/data/edge.db")
MAX_OUTBOX_EVENTS = max(1, int(os.getenv("MAX_OUTBOX_EVENTS", "1000")))
HTTP_PORT = int(os.getenv("HTTP_PORT", "8081"))

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
db = core.connect(DB_PATH)


class Api(BaseHTTPRequestHandler):
    """Read-only status API used by scripts/run_experiments.py."""

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
        conn = core.connect(DB_PATH)
        try:
            if url.path == "/metrics":
                self._send(200, core.metrics(conn, MAX_OUTBOX_EVENTS))
            elif url.path == "/state":
                self._send(200, core.twin_state(conn))
            elif url.path == "/event_ids":
                self._send(200, core.event_ids(conn, (q.get("since") or [None])[0]))
            else:
                self._send(404, {"error": "not found"})
        finally:
            conn.close()

    def log_message(self, *args):
        pass


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"MQTT connected: {reason_code}", flush=True)
    if reason_code == 0:
        client.subscribe("factory/+/telemetry", qos=1)


def on_message(client, userdata, message):
    try:
        event = json.loads(message.payload.decode())
        res = core.apply_event(db, event, MAX_OUTBOX_EVENTS, origin="edge")
        extra = f" evicted={res['evicted']}" if res["evicted"] else ""
        print(f"{res['outcome'].upper()} {event['twin_id']}/{event['property']} "
              f"id={event['event_id']}{extra}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR processing MQTT message: {exc}", flush=True)


if __name__ == "__main__":
    threading.Thread(
        target=lambda: ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Api).serve_forever(),
        daemon=True).start()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="cc-mp-edge")
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=10)
    print(f"Edge started. DB={DB_PATH} outbox capacity={MAX_OUTBOX_EVENTS}",
          flush=True)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_forever()
