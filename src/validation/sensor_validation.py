REQUIRED_FIELDS = [
    "event_id", "sensor_id", "timestamp", "temperature_f",
    "humidity_pct", "wind_mph", "precip_mm", "battery_level",
    "gps", "region", "schema_version"
]

def is_valid_gps(gps: dict) -> bool:
    if not isinstance(gps, dict):
        return False
    lat = gps.get("lat")
    lon = gps.get("lon")
    if lat is None or lon is None:
        return False
    try:
        lat = float(lat)
        lon = float(lon)
    except:
        return False
    return (-90 <= lat <= 90) and (-180 <= lon <= 180)

def basic_validate_sensor(event: dict):
    errors = []

    # required fields
    for f in REQUIRED_FIELDS:
        if f not in event:
            errors.append(f"missing_field:{f}")

    if event.get("schema_version") == 2:
        if "firmware_version" not in event:
            errors.append("missing_firmware_version")

    # temperature range
    try:
        t = float(event.get("temperature_f"))
        if not (-50 <= t <= 150):
            errors.append("temp_out_of_range")
    except:
        errors.append("temp_invalid_type")

    # battery numeric (int or float allowed for schema evolution)
    try:
        float(event.get("battery_level"))
    except:
        errors.append("battery_invalid_type")

    # GPS validity
    if not is_valid_gps(event.get("gps")):
        errors.append("gps_invalid")

    return (len(errors) == 0), errors