import os
import pandas as pd
import s3fs
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def clamp(x, lo=0, hi=100):
    return max(lo, min(hi, x))

def main(date=None):
    if not date:
        date = datetime.utcnow().strftime("%Y-%m-%d")

    fs = s3fs.S3FileSystem(
        key=os.getenv("MINIO_ACCESS_KEY"),
        secret=os.getenv("MINIO_SECRET_KEY"),
        client_kwargs={"endpoint_url": os.getenv("MINIO_ENDPOINT")}
    )

    bucket = os.getenv("MINIO_BUCKET")

    prefix = f"{bucket}/data-lake/clean/weather_standardized/"
    files = fs.glob(prefix + f"region=*/date={date}/*.parquet")

    if not files:
        print("No weather data found for date:", date)
        return

    df_list = []
    for f in files:
        with fs.open(f, "rb") as file:
            df_list.append(pd.read_parquet(file))

    df = pd.concat(df_list, ignore_index=True)

    df["risk_score"] = df.apply(
        lambda r: clamp(
            (0.4 * float(r.get("temperature_2m") or 0)) +
            (0.4 * float(r.get("wind_speed_10m") or 0)) +
            (0.2 * float(r.get("precipitation") or 0))
        ),
        axis=1
    )

    output_key = f"{bucket}/data-lake/curated/environmental_risk_index/date={date}/part-risk.parquet"

    with fs.open(output_key, "wb") as f:
        df.to_parquet(f, index=False)

    print("Environmental risk index written successfully.")

if __name__ == "__main__":
    main()