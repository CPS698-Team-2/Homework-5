import os
import json
import uuid
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer

from src.kafka_config import confluent_config
from src.validation.sensor_validation import basic_validate_sensor
from src.storage.minio_writer import get_minio_fs, write_jsonl

load_dotenv()

TOPIC = "synthetic_sensors_raw"


def date_from_ts(ts_iso: str) -> str:
    # Expect ISO timestamp like "2026-03-02T..."
    # Fallback to today's date if missing/bad
    if isinstance(ts_iso, str) and len(ts_iso) >= 10:
        return ts_iso[:10]
    return datetime.utcnow().strftime("%Y-%m-%d")


def run():
    # MinIO FS
    fs = get_minio_fs(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
    )
    bucket = os.getenv("MINIO_BUCKET")

    # Kafka consumer
    conf = confluent_config()
    conf.update(
        {
            "group.id": "sensor-raw-consumer-group",
            "auto.offset.reset": "earliest",  # keep earliest to demonstrate bad_records handling
        }
    )
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    valid_batch = []
    invalid_batch = []

    print("Consuming from Kafka topic:", TOPIC)
    print("Writing to MinIO bucket:", bucket)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                # flush buffered messages periodically
                if valid_batch or invalid_batch:
                    flush_batches(fs, bucket, valid_batch, invalid_batch)
                    valid_batch.clear()
                    invalid_batch.clear()
                continue

            if msg.error():
                print("Kafka error:", msg.error())
                continue

            # --- SAFE MESSAGE PARSING (handles non-JSON / empty payloads) ---
            raw_bytes = msg.value()

            if raw_bytes is None or len(raw_bytes) == 0:
                invalid_batch.append(
                    {
                        "_errors": ["empty_message"],
                        "_raw": None,
                        "_topic": msg.topic(),
                        "_partition": msg.partition(),
                        "_offset": msg.offset(),
                        "_ts_utc": datetime.utcnow().isoformat(),
                    }
                )
                continue

            raw_text = raw_bytes.decode("utf-8", errors="replace")

            try:
                event = json.loads(raw_text)
            except json.JSONDecodeError:
                invalid_batch.append(
                    {
                        "_errors": ["invalid_json"],
                        "_raw": raw_text,
                        "_topic": msg.topic(),
                        "_partition": msg.partition(),
                        "_offset": msg.offset(),
                        "_ts_utc": datetime.utcnow().isoformat(),
                    }
                )
                continue

            # --- VALIDATION ---
            ok, errors = basic_validate_sensor(event)
            if ok:
                valid_batch.append(event)
            else:
                event["_errors"] = errors
                invalid_batch.append(event)

            # flush thresholds
            if len(valid_batch) >= 200 or len(invalid_batch) >= 50:
                flush_batches(fs, bucket, valid_batch, invalid_batch)
                valid_batch.clear()
                invalid_batch.clear()

    except KeyboardInterrupt:
        if valid_batch or invalid_batch:
            flush_batches(fs, bucket, valid_batch, invalid_batch)

    finally:
        consumer.close()
        print("Consumer closed.")


def flush_batches(fs, bucket, valid_batch, invalid_batch):
    run_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()

    print(f"\nFlushing batches run_id={run_id} @ {now}")
    print("Valid:", len(valid_batch), "| Invalid:", len(invalid_batch))

    # write valid RAW
    if valid_batch:
        day = date_from_ts(valid_batch[0].get("timestamp"))
        key = f"data-lake/raw/synthetic_sensors/date={day}/run={run_id}.jsonl"
        write_jsonl(fs, bucket, key, valid_batch)
        print("Wrote valid RAW:", key)

    # write invalid RAW (bad records)
    if invalid_batch:
        # Use timestamp if present in invalid event, else today's date
        first = invalid_batch[0]
        day = date_from_ts(first.get("timestamp") if isinstance(first, dict) else None)
        key = f"data-lake/raw/bad_records/source=sensors/date={day}/run={run_id}.jsonl"
        write_jsonl(fs, bucket, key, invalid_batch)
        print("Wrote BAD records:", key)


if __name__ == "__main__":
    run()