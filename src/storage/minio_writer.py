import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

# ---------- Preferred MinIO-style names ----------
def get_minio_fs(endpoint: str, access_key: str, secret_key: str):
    return s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint}
    )

def write_jsonl(fs: s3fs.S3FileSystem, bucket: str, key: str, records: list):
    """
    Writes JSON Lines to: {bucket}/{key}
    """
    path = f"{bucket}/{key}"
    with fs.open(path, "wb") as f:
        for r in records:
            f.write((json.dumps(r) + "\n").encode("utf-8"))

def write_parquet(fs: s3fs.S3FileSystem, bucket: str, key: str, df: pd.DataFrame, compression: str = "snappy"):
    """
    Writes Parquet (Snappy) to: {bucket}/{key}
    """
    path = f"{bucket}/{key}"
    table = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(path, "wb") as f:
        pq.write_table(table, f, compression=compression)

# ---------- Backward-compatible names (so your consumers work) ----------
def make_s3fs(endpoint_url: str, key: str, secret: str):
    """
    Alias used by consumer code: make_s3fs(endpoint_url, key, secret)
    """
    return get_minio_fs(endpoint=endpoint_url, access_key=key, secret_key=secret)

def write_json_lines(fs: s3fs.S3FileSystem, bucket: str, key: str, records: list):
    """
    Alias used by consumer code: write_json_lines(...)
    """
    return write_jsonl(fs, bucket, key, records)