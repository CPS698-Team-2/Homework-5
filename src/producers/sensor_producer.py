import json
import time
import random
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from src.kafka_config import confluent_config

TOPIC = "synthetic_sensors_raw"

def make_sensor_event(sensor_id: int, evolved=False):
    event = {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_f": round(random.uniform(-20, 120), 2),
        "humidity_pct": round(random.uniform(5, 95), 2),
        "wind_mph": round(random.uniform(0, 60), 2),
        "precip_mm": round(max(0, random.gauss(1, 2)), 2),
        "battery_level": random.randint(0, 100),  # schema v1
        "gps": {
            "lat": round(random.uniform(25.0, 49.0), 6),
            "lon": round(random.uniform(-124.0, -66.0), 6)
        },
        "region": random.choice(["midwest", "south", "west", "northeast"]),
        "schema_version": 1
    }

    # Schema evolution simulation
    if evolved:
        event["firmware_version"] = random.choice(["1.0.2", "1.1.0", "2.0.0"])
        event["battery_level"] = round(random.uniform(0, 100), 2)  # int → float
        event["schema_version"] = 2

    return event

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Sent to {msg.topic()} offset {msg.offset()}")

def run():
    producer = Producer(confluent_config())

    for i in range(200):
        sensor_id = random.randint(1, 5)
        evolved = i >= 120  # after 120 records switch schema
        event = make_sensor_event(sensor_id, evolved)
        producer.produce(
            TOPIC,
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(0.05)

    producer.flush()
    print("Finished producing sensor data.")

if __name__ == "__main__":
    run()