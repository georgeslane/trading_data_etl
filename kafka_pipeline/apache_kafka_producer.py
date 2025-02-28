import websocket
import json
from websocket._app import WebSocketApp
from kafka import KafkaConsumer, KafkaProducer


class WebSocketConnection:
    def __init__(self, kafka_producer, api_key, topic, ticker_symbols):
        self.kafka_producer = kafka_producer
        self.api_key = api_key
        self.topic = topic
        self.ticker_sysmbols = ticker_symbols

    def on_message(self, ws, message):
        self.kafka_producer.send(self.topic, json.loads(message))

    def on_error(self, ws, error):
        print("WebSocket Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket Closed")

    def on_open(self, ws):
        subscriptions = []
        for symbol in self.ticker_symbols:
            subscriptions.append(f'{{"type":"subscribe","symbol":"{symbol}"}}')
        
        for item in subscriptions:
            ws.send(item)

    def start_websocket(self):
        ws = WebSocketApp(
            f"wss://ws.finnhub.io?token={self.api_key}",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        ws.on_open = self.on_open
        ws.run_forever()


class KafkaServer:
    def __init__(self, broker, topic):
        self.topic = topic
        self.broker = broker
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.broker,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def get_messages(self):
        for message in self.consumer:
            print(message.value)