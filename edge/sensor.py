import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
INTERVAL = float(os.getenv("SENSOR_INTERVAL", "0.1"))

sensors = [
    ("motor-01", "temperature", 60.0, 90.0),
    ("motor-01", "vibration", 0.1, 1.0),
    ("pump-01", "pressure", 2.0, 8.0),
    ("pump-01", "flow_rate", 10.0, 50.0),
    ("tank-01", "level", 20.0, 100.0),
    ("tank-01", "temperature", 15.0, 35.0),
]

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="cc-mp-sensor-simulator"
)

while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
        break
    except OSError as exc:
        print(f"Waiting for MQTT broker: {exc}", flush=True)
        time.sleep(2)

client.loop_start()

print("Sensor simulator running", flush=True)

index = 0

try:
    while True:
        twin_id, prop, minimum, maximum = sensors[index % len(sensors)]

        event = {
            "event_id": str(uuid.uuid4()),
            "twin_id": twin_id,
            "property": prop,
            "value": round(random.uniform(minimum, maximum), 2),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }

        topic = f"factory/{twin_id}/telemetry"

        info = client.publish(
            topic,
            json.dumps(event),
            qos=1
        )

        info.wait_for_publish(timeout=5)

        print(f"PUBLISHED {topic}: {json.dumps(event)}", flush=True)

        index += 1
        time.sleep(INTERVAL)

except KeyboardInterrupt:
    pass
finally:
    client.loop_stop()
    client.disconnect()
