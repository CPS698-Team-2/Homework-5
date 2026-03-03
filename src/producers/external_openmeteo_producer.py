import json
from datetime import datetime, timezone
import requests
from confluent_kafka import Producer
from src.kafka_config import confluent_config

WEATHER_TOPIC = "external_weather_raw"
AIR_TOPIC = "external_air_quality_raw"

# Regions (add more if you want)
REGIONS = [
    {"region": "midwest", "lat": 43.6, "lon": -84.8},    # Michigan
    {"region": "west", "lat": 34.05, "lon": -118.25},    # LA
    {"region": "northeast", "lat": 40.71, "lon": -74.01} # NYC
]

def fetch_weather(lat, lon):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,wind_speed_10m,precipitation"
    )
    return requests.get(url, timeout=20).json()

def fetch_air(lat, lon):
    url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={lat}&longitude={lon}"
        "&current=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone"
    )
    return requests.get(url, timeout=20).json()

def run():
    p = Producer(confluent_config())
    ts = datetime.now(timezone.utc).isoformat()

    for r in REGIONS:
        # Weather event
        weather_payload = fetch_weather(r["lat"], r["lon"])
        weather_event = {
            "source": "open-meteo",
            "region": r["region"],
            "timestamp": ts,
            "lat": r["lat"],
            "lon": r["lon"],
            "payload": weather_payload
        }
        p.produce(WEATHER_TOPIC, value=json.dumps(weather_event).encode("utf-8"))

        # Air event
        air_payload = fetch_air(r["lat"], r["lon"])
        air_event = {
            "source": "open-meteo",
            "region": r["region"],
            "timestamp": ts,
            "lat": r["lat"],
            "lon": r["lon"],
            "payload": air_payload
        }
        p.produce(AIR_TOPIC, value=json.dumps(air_event).encode("utf-8"))

        p.poll(0)

    p.flush()
    print("External weather + air quality pushed to Kafka successfully.")

if __name__ == "__main__":
    run()