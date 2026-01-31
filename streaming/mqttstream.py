import json
import random
import time
from datetime import datetime, timedelta, timezone
import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883

# Topic pattern: denbosch/sensors/air/<sensor_id>
TOPIC_PREFIX = "denbosch/sensors/air"

LAT_RANGE = (51.683, 51.733)
LON_RANGE = (5.283, 5.383)

FLEET_SIZE = 25               # realistic pilot fleet
BASE_INTERVAL_SEC = 10        # average publish interval
JITTER_SEC = 3                # timing randomness
DROP_PROB = 0.03              # chance to skip a publish
BAD_PAYLOAD_PROB = 0.01       # chance to publish malformed json
BURST_PROB = 0.02             # chance to publish buffered burst
MAX_BURST = 10                # up to N buffered messages

CLOCK_SKEW_SEC_RANGE = (-30, 30)  # device clock error

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def random_location():
    return {
        "lat": round(random.uniform(*LAT_RANGE), 6),
        "lon": round(random.uniform(*LON_RANGE), 6),
    }

def generate_reading(sensor_id: str, skew_seconds: int):
    # device time with skew
    ts = datetime.now(timezone.utc) + timedelta(seconds=skew_seconds)

    temperature = round(random.uniform(18.0, 35.0), 2)
    humidity = round(random.uniform(35.0, 85.0), 2)
    co2_ppm = int(random.uniform(380, 1200))
    noise_dba = round(random.uniform(35.0, 85.0), 1)

    payload = {
        "sensor_id": sensor_id,
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
        "temperature": temperature,
        "humidity": humidity,
        "co2_ppm": co2_ppm,
        "noise_dba": noise_dba,
        "location": random_location(),
        "battery": random.randint(20, 100),
        "fw": "sim-1.0",
    }
    return payload

def publish(client, topic, payload, qos=1):
    client.publish(topic, json.dumps(payload), qos=qos)

def publish_bad_payload(client, topic, qos=1):
    # intentionally broken data
    junk = random.choice([
        b"{not_json",
        b"null",
        b'{"sensor_id": 123, "timestamp": "???"}',  # wrong types
        b'{"temperature": "hot"}'                   # missing fields
    ])
    client.publish(topic, junk, qos=qos)

def main():
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()

    sensors = []
    for i in range(1, FLEET_SIZE + 1):
        sensor_id = f"sensor_{i:04d}"
        sensors.append({
            "sensor_id": sensor_id,
            "skew": random.randint(*CLOCK_SKEW_SEC_RANGE),
            "buffer": [],
            "stuck_mode": False,
            "stuck_value": None,
        })

    print(f"Simulating {FLEET_SIZE} sensors publishing to MQTT {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Topic prefix: {TOPIC_PREFIX}/<sensor_id>")

    try:
        while True:
            for s in sensors:
                sensor_id = s["sensor_id"]
                topic = f"{TOPIC_PREFIX}/{sensor_id}"

                # Randomly enter "stuck sensor" mode for a while
                if not s["stuck_mode"] and random.random() < 0.001:
                    s["stuck_mode"] = True
                    s["stuck_value"] = generate_reading(sensor_id, s["skew"])
                elif s["stuck_mode"] and random.random() < 0.01:
                    s["stuck_mode"] = False
                    s["stuck_value"] = None

                # Decide if message drops
                if random.random() < DROP_PROB:
                    continue

                # Bad payload occasionally
                if random.random() < BAD_PAYLOAD_PROB:
                    publish_bad_payload(client, topic, qos=1)
                    continue

                # Normal reading (or stuck reading)
                if s["stuck_mode"]:
                    payload = s["stuck_value"]
                    payload["timestamp"] = utc_now_iso()  # timestamp still moves, value stuck
                else:
                    payload = generate_reading(sensor_id, s["skew"])

                # Sometimes buffer instead of sending (simulates offline buffering)
                if random.random() < 0.02:
                    s["buffer"].append(payload)
                else:
                    publish(client, topic, payload, qos=1)

                # Sometimes flush a burst (reconnect + buffer dump)
                if s["buffer"] and random.random() < BURST_PROB:
                    burst_n = min(len(s["buffer"]), random.randint(2, MAX_BURST))
                    for _ in range(burst_n):
                        publish(client, topic, s["buffer"].pop(0), qos=1)

            # Global pacing with jitter (not perfectly periodic)
            sleep_for = max(0.2, BASE_INTERVAL_SEC + random.uniform(-JITTER_SEC, JITTER_SEC))
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        print("Stopping simulator...")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
