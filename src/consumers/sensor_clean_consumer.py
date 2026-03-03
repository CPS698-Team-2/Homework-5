import os
import json
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer

from src.kafka_config import confluent_config
from src.validation.ge_validator import validate_sensor_dataframe
from src.storage.minio_writer import get_minio_fs

load_dotenv()

TOPIC = "synthetic_sensors_raw"

def run():
    fs = get_minio_fs(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
    )
    bucket = os.getenv("MINIO_BUCKET")

    conf = confluent_config()
    conf.update({
        "group.id": "sensor-clean-consumer-group",
        "auto.offset.reset": "earliest"
    })

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    batch = []

    print("Starting CLEAN consumer...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                if batch:
                    process_batch(fs, bucket, batch)
                    batch.clear()
                continue

            if msg.error():
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
                batch.append(event)
            except:
                continue

            if len(batch) >= 200:
                process_batch(fs, bucket, batch)
                batch.clear()

    except KeyboardInterrupt:
        if batch:
            process_batch(fs, bucket, batch)
    finally:
        consumer.close()

def process_batch(fs, bucket, batch):
    df = pd.DataFrame(batch)

    # Handle schema evolution safely
    if "firmware_version" not in df.columns:
        df["firmware_version"] = None

    # Ensure battery_level is float
    df["battery_level"] = df["battery_level"].astype(float)

    success, results = validate_sensor_dataframe(df)

    if not success:
        print("Great Expectations validation failed.")
        return

    df["date"] = df["timestamp"].str.slice(0, 10)

    run_id = str(uuid.uuid4())[:8]

    for (date, sensor_id), group in df.groupby(["date", "sensor_id"]):
        path = f"{bucket}/data-lake/clean/sensors_validated/date={date}/sensor_id={sensor_id}/part-{run_id}.parquet"

        table = pa.Table.from_pandas(group.drop(columns=["date"]), preserve_index=False)

        with fs.open(path, "wb") as f:
            pq.write_table(table, f, compression="snappy")

        print("Wrote CLEAN parquet:", path)

if __name__ == "__main__":
    run()