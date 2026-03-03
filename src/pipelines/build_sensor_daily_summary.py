import os
import pandas as pd
import s3fs
from dotenv import load_dotenv

load_dotenv()

def main(date=None):
    if not date:
        from datetime import datetime
        date = datetime.utcnow().strftime("%Y-%m-%d")

    fs = s3fs.S3FileSystem(
        key=os.getenv("MINIO_ACCESS_KEY"),
        secret=os.getenv("MINIO_SECRET_KEY"),
        client_kwargs={"endpoint_url": os.getenv("MINIO_ENDPOINT")}
    )

    bucket = os.getenv("MINIO_BUCKET")

    prefix = f"{bucket}/data-lake/clean/sensors_validated/date={date}/"
    files = fs.glob(prefix + "**/*.parquet")

    if not files:
        print("No clean files found for date:", date)
        return

    df_list = []
    for f in files:
        with fs.open(f, "rb") as file:
            df_list.append(pd.read_parquet(file))

    df = pd.concat(df_list, ignore_index=True)

    summary = (
        df.groupby("sensor_id")
          .agg(
              avg_temp=("temperature_f","mean"),
              max_temp=("temperature_f","max"),
              avg_wind=("wind_mph","mean"),
              avg_humidity=("humidity_pct","mean"),
              avg_battery=("battery_level","mean"),
              record_count=("event_id","count")
          )
          .reset_index()
    )

    summary["date"] = date

    output_key = f"{bucket}/data-lake/curated/sensor_daily_summary/date={date}/part-summary.parquet"

    with fs.open(output_key, "wb") as f:
        summary.to_parquet(f, index=False)

    print("Sensor daily summary written successfully.")

if __name__ == "__main__":
    main()