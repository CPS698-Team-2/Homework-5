import os, json, uuid
from datetime import datetime
import pandas as pd
from confluent_kafka import Consumer
from dotenv import load_dotenv

from src.kafka_config import confluent_config
from src.validation.external_validation import validate_external_event
from src.storage.minio_writer import make_s3fs, write_json_lines, write_parquet

load_dotenv()
TOPIC = "external_weather_raw"

def get_date(ts):
    return ts[:10]

def run():
    fs = make_s3fs(
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        key=os.getenv("MINIO_ACCESS_KEY"),
        secret=os.getenv("MINIO_SECRET_KEY"),
    )
    bucket = os.getenv("MINIO_BUCKET")

    conf = confluent_config()
    conf.update({"group.id": "ext-weather-group", "auto.offset.reset": "earliest"})
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    batch_valid = []
    batch_invalid = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if batch_valid or batch_invalid:
                    flush(fs, bucket, batch_valid, batch_invalid)
                    batch_valid.clear()
                    batch_invalid.clear()
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            event = json.loads(msg.value().decode("utf-8"))
            ok, errors = validate_external_event(event, kind="weather")
            if ok:
                batch_valid.append(event)
            else:
                event["_errors"] = errors
                batch_invalid.append(event)

            if len(batch_valid) >= 20 or len(batch_invalid) >= 10:
                flush(fs, bucket, batch_valid, batch_invalid)
                batch_valid.clear()
                batch_invalid.clear()

    except KeyboardInterrupt:
        flush(fs, bucket, batch_valid, batch_invalid)
    finally:
        consumer.close()

def flush(fs, bucket, valid, invalid):
    run_id = str(uuid.uuid4())[:8]
    print(f"Flushing weather batch run={run_id} valid={len(valid)} invalid={len(invalid)}")

    # RAW writes (partition by region then date)
    for items, source in [(valid, "external_weather"), (invalid, "bad_records/source=external_weather")]:
        if not items:
            continue
        # group by region/date
        for (region, day), group in group_events(items):
            key = f"data-lake/raw/{source}/region={region}/date={day}/run={run_id}.jsonl"
            write_json_lines(fs, bucket, key, group)

    # CLEAN writes
    if valid:
        rows = []
        for e in valid:
            current = (e.get("payload") or {}).get("current", {}) or {}
            rows.append({
                "timestamp": e["timestamp"],
                "region": e["region"],
                "lat": e["lat"],
                "lon": e["lon"],
                "temperature_2m": current.get("temperature_2m"),
                "wind_speed_10m": current.get("wind_speed_10m"),
                "precipitation": current.get("precipitation"),
                "source": e.get("source", "open-meteo")
            })
        df = pd.DataFrame(rows)
        df["date"] = df["timestamp"].str.slice(0, 10)

        for (region, day), part in df.groupby(["region", "date"]):
            key = f"data-lake/clean/weather_standardized/region={region}/date={day}/part-{run_id}.parquet"
            write_parquet(fs, bucket, key, part.drop(columns=["date"]))

def group_events(items):
    buckets = {}
    for e in items:
        region = e.get("region", "unknown")
        day = get_date(e.get("timestamp", datetime.utcnow().isoformat()))
        buckets.setdefault((region, day), []).append(e)
    return buckets.items()

if __name__ == "__main__":
    run()