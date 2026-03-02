# Environmental Data Lake Pipeline (Kafka + External APIs)

## Data Lake Layout
/data-lake
  /raw
    /synthetic_sensors
    /external_weather
    /external_air_quality
    /bad_records
  /clean
    /sensors_validated
    /weather_standardized
  /curated
    /sensor_daily_summary
    /environmental_risk_index

## Formats
- Raw: JSONL (or CSV)
- Clean/Curated: Parquet (Snappy)

## Partitioning
- Sensors: /clean/sensors_validated/date=YYYY-MM-DD/sensor_id=X/
- External: /clean/weather_standardized/region=R/date=YYYY-MM-DD/

## Validation
- Range checks (temp -50..150, etc.)
- GPS validity
- Great Expectations batch checks
- Invalid data routed to /raw/bad_records