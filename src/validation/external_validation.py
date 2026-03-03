def validate_external_event(event: dict, kind: str):
    """
    kind: 'weather' or 'air'
    """
    errors = []

    # Common checks
    for f in ["source", "region", "timestamp", "payload", "lat", "lon"]:
        if f not in event:
            errors.append(f"missing_field:{f}")

    # Lat/Lon validity
    try:
        lat = float(event.get("lat"))
        lon = float(event.get("lon"))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            errors.append("invalid_latlon_range")
    except:
        errors.append("invalid_latlon_type")

    # Payload basic structure check
    payload = event.get("payload", {})
    current = (payload or {}).get("current", None)
    if current is None:
        errors.append("missing_payload_current")

    # Weather-specific: must contain at least one of these
    if kind == "weather" and current:
        if not any(k in current for k in ["temperature_2m", "wind_speed_10m", "precipitation"]):
            errors.append("weather_current_missing_expected_fields")

    # Air-specific: must contain at least one of these
    if kind == "air" and current:
        if not any(k in current for k in ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "ozone"]):
            errors.append("air_current_missing_expected_fields")

    return (len(errors) == 0), errors