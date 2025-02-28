import json
import threading
import websocket
from kafka import KafkaProducer

# Finnhub WebSocket URL (replace with your API key)
FINNHUB_WS_URL = "wss://ws.finnhub.io?token=YOUR_FINNHUB_API_KEY"

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'finnhub_stock_data'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# WebSocket Message Handler
def on_message(ws, message):
    print("Received:", message)
    producer.send(KAFKA_TOPIC, json.loads(message))  # Send to Kafka

def on_open(ws):
    print("Connected to Finnhub WebSocket")
    # Subscribe to stock updates (example: Apple - AAPL)
    ws.send(json.dumps({"type": "subscribe", "symbol": "AAPL"}))

def on_error(ws, error):
    print("WebSocket Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket Closed")

# Start Finnhub WebSocket connection
def start_websocket():
    ws = websocket.WebSocketApp(FINNHUB_WS_URL, 
                                on_message=on_message, 
                                on_open=on_open, 
                                on_error=on_error, 
                                on_close=on_close)
    ws.run_forever()

# Run WebSocket in a separate thread
threading.Thread(target=start_websocket, daemon=True).start()