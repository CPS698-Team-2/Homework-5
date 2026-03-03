from confluent_kafka import Producer
from src.kafka_config import confluent_config

conf = confluent_config()

p = Producer(conf)

p.produce("synthetic_sensors_raw", value=b"test message")
p.flush()

print("Sent OK")