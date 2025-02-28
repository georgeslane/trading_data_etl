from kafka import KafkaConsumer
import json

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'webhook_events'

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening for webhook events...")

for message in consumer:
    event_data = message.value
    print("Received Webhook Event:", event_data)