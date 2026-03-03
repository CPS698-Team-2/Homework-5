

import os
import json
import uuid
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from confluent_kafka import Consumer
from dotenv import load_dotenv

from src.kafka_config import confluent_config
from src.validation.sensor_validation import basic_validate_sensor
from src.validation.ge_runner import validate_sensor_batch  # batch-level GE checks


TOPIC = "synthetic_sensors_raw"


def _date_from_iso(ts: str) -> str:
    # expected: "YYYY-MM-DD..."
    return str(ts)[:10] if ts else datetime.utcnow().strftime("%Y-%m-%d")


def _make_minio_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=os.getenv("MINIO_ACCESS_KEY"),
        secret=os.getenv("MINIO_SECRET_KEY"),
        client_kwargs={"endpoint_url": os.getenv("MINIO_ENDPOINT")},
    )


def _write_jsonl(fs: s3fs.S3FileSystem, bucket: str, key: str, records: list[dict]) -> None:
    """
    Writes JSON Lines to s3://<bucket>/<key> using the provided filesystem.
    """
    path = f"{bucket}/{key}"
    with fs.open(path, "wb") as f:
        for r in records:
            f.write((json.dumps(r) + "\n").encode("utf-8"))


def _write_parquet(fs: s3fs.S3FileSystem, bucket: str, key: str, df: pd.DataFrame) -> None:
    """
    Writes Parquet (Snappy) to s3://<bucket>/<key>.
    """
    path = f"{bucket}/{key}"
    table = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(path, "wb") as f:
        pq.write_table(table, f, compression="snappy")


def _zscore_outliers(df: pd.DataFrame, col: str, threshold: float = 3.0) -> pd.DataFrame:
    """
    Simple outlier detection: |z| > threshold.
    Returns the rows considered outliers (can be empty).
    """
    if col not in df.columns:
        return df.iloc[0:0]
    series = pd.to_numeric(df[col], errors="coerce")
    if series.std(skipna=True) == 0 or series.isna().all():
        return df.iloc[0:0]
    z = (series - series.mean(skipna=True)) / series.std(skipna=True)
    return df[abs(z) > threshold]


def flush_batches(
    fs: s3fs.S3FileSystem,
    bucket: str,
    valid_events: list[dict],
    invalid_events: list[dict],
) -> None:
    """
    1) Write RAW valid JSONL to /raw/synthetic_sensors/
    2) Write RAW invalid JSONL to /raw/bad_records/
    3) Run GE on valid batch; if GE fails -> move to bad_records
    4) Outlier detection; outliers -> bad_records
    5) Write CLEAN Parquet partitioned by date and sensor_id
    """
    if not valid_events and not invalid_events:
        return

    run_id = str(uuid.uuid4())[:8]
    print(f"[flush] run={run_id} valid={len(valid_events)} invalid={len(invalid_events)}")

    # Decide day using first available event
    sample_ts = None
    if valid_events:
        sample_ts = valid_events[0].get("timestamp")
    elif invalid_events:
        sample_ts = invalid_events[0].get("timestamp")
    day = _date_from_iso(sample_ts)

    # --- RAW writes ---
    if valid_events:
        raw_key = f"data-lake/raw/synthetic_sensors/date={day}/run={run_id}.jsonl"
        _write_jsonl(fs, bucket, raw_key, valid_events)

    if invalid_events:
        bad_key = f"data-lake/raw/bad_records/source=sensors/date={day}/run={run_id}.jsonl"
        _write_jsonl(fs, bucket, bad_key, invalid_events)

    # If nothing valid, stop here
    if not valid_events:
        return

    # --- Batch validation (Great Expectations) ---
    df = pd.DataFrame(valid_events)

    # ensure numeric columns are numeric (helps GE checks)
    for col in ["temperature_f", "humidity_pct", "wind_mph", "precip_mm", "battery_level"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    ge_ok, ge_results = validate_sensor_batch(df)
    if not ge_ok:
        # If GE fails, route entire batch to bad_records (strict approach)
        routed = []
        for r in valid_events:
            r["_errors"] = r.get("_errors", []) + ["great_expectations_failed"]
            routed.append(r)

        bad_key = f"data-lake/raw/bad_records/source=sensors_ge/date={day}/run={run_id}.jsonl"
        _write_jsonl(fs, bucket, bad_key, routed)
        print("[flush] GE failed -> routed valid batch to bad_records; skipping clean write.")
        return

    # --- Outlier detection (temperature) ---
    out_df = _zscore_outliers(df, "temperature_f", threshold=3.0)
    if not out_df.empty and "event_id" in out_df.columns:
        outlier_ids = set(out_df["event_id"].astype(str).tolist())

        # split outliers vs remaining
        remaining = []
        outliers = []
        for r in valid_events:
            if str(r.get("event_id")) in outlier_ids:
                r["_errors"] = r.get("_errors", []) + ["temp_outlier_zscore"]
                outliers.append(r)
            else:
                remaining.append(r)

        if outliers:
            bad_key = f"data-lake/raw/bad_records/source=sensors_outliers/date={day}/run={run_id}.jsonl"
            _write_jsonl(fs, bucket, bad_key, outliers)
            print(f"[flush] routed outliers={len(outliers)} to bad_records")

        valid_events = remaining
        if not valid_events:
            print("[flush] no remaining valid events after outlier routing; skipping clean write.")
            return

        df = pd.DataFrame(valid_events)

    # --- CLEAN writes (Parquet, partitioned by date + sensor_id) ---
    # Add helper partition column
    if "timestamp" in df.columns:
        df["date"] = df["timestamp"].astype(str).str.slice(0, 10)
    else:
        df["date"] = day

    # Ensure sensor_id is present for partitioning
    if "sensor_id" not in df.columns:
        # if this happens, route to bad_records
        routed = []
        for r in valid_events:
            r["_errors"] = r.get("_errors", []) + ["missing_sensor_id_for_partition"]
            routed.append(r)
        bad_key = f"data-lake/raw/bad_records/source=sensors_partition/date={day}/run={run_id}.jsonl"
        _write_jsonl(fs, bucket, bad_key, routed)
        print("[flush] missing sensor_id -> routed to bad_records; skipping clean write.")
        return

    for (d, sid), part in df.groupby(["date", "sensor_id"], dropna=False):
        # defensive casts
        d = str(d)[:10]
        sid = int(sid) if pd.notna(sid) else -1

        clean_key = f"data-lake/clean/sensors_validated/date={d}/sensor_id={sid}/part-{run_id}.parquet"
        # do not store helper date column in parquet
        out_part = part.drop(columns=["date"], errors="ignore")
        _write_parquet(fs, bucket, clean_key, out_part)

    print("[flush] wrote clean parquet partitions successfully.")


def run():
    load_dotenv()

    # MinIO config
    bucket = os.getenv("MINIO_BUCKET")
    if not bucket:
        raise RuntimeError("MINIO_BUCKET is not set in .env")

    fs = _make_minio_fs()

    # Kafka consumer config
    cconf = confluent_config()
    cconf.update(
        {
            "group.id": "sensor-cleaning-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )

    consumer = Consumer(cconf)
    consumer.subscribe([TOPIC])

    valid_events: list[dict] = []
    invalid_events: list[dict] = []

    # Batch thresholds
    VALID_BATCH_SIZE = 200
    INVALID_BATCH_SIZE = 50

    print(f"[start] consuming topic={TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                # flush any accumulated batches during idle
                if valid_events or invalid_events:
                    flush_batches(fs, bucket, valid_events, invalid_events)
                    valid_events.clear()
                    invalid_events.clear()
                continue

            if msg.error():
                print("[kafka] error:", msg.error())
                continue

            # Parse event
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                invalid_events.append({"_errors": ["json_decode_error"], "raw": str(msg.value()), "exception": str(e)})
                continue

            # Per-record validation
            ok, errors = basic_validate_sensor(event)
            if ok:
                valid_events.append(event)
            else:
                event["_errors"] = errors
                invalid_events.append(event)

            # flush based on thresholds
            if len(valid_events) >= VALID_BATCH_SIZE or len(invalid_events) >= INVALID_BATCH_SIZE:
                flush_batches(fs, bucket, valid_events, invalid_events)
                valid_events.clear()
                invalid_events.clear()

    except KeyboardInterrupt:
        print("[stop] keyboard interrupt, flushing remaining batches...")
        flush_batches(fs, bucket, valid_events, invalid_events)

    finally:
        consumer.close()
        print("[stop] consumer closed.")


if __name__ == "__main__":
    run()