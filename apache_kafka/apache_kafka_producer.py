import websocket
import json
from websocket._app import WebSocketApp
from kafka import KafkaConsumer, KafkaProducer


class WebSocketConnection:
    def __init__(self, kafka_producer, api_key, ticker_symbols):
        self.kafka_producer = kafka_producer
        self.api_key = api_key
        self.ticker_symbols = ticker_symbols

    def on_message(self, ws, message):
        json_message = json.loads(message)
        data = json_message['data']
        for item in data:
            topic = str(item['s']).replace(':', '_')
            self.kafka_producer.send(topic, item)

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
            on_close=self.on_close,
        )
        ws.on_open = self.on_open
        ws.run_forever()


class KafkaServer:
    def __init__(self, broker, topics):
        self.topics = self.get_topics(topics)
        self.broker = broker
        self.consumers = self.create_consumers()
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def insert_data_into_raw_tables(self, tables_dictionary):
        """insert_data_into_raw_tables() inserts records from each consumer object in self.consumers into a corresponsing table.
        
        Args:
            tables_dictionary (dictionary): Defines a dictionary of tables, where each table is identified by a Kafka topic."""
        for topic in self.consumers:
            for message in self.consumers[topic]:
                tables_dictionary[f'raw_{topic}'].insert_record(message.value)

    def get_topics(self, topics):
        """get_topics() replaces invalid chars in a list of topics with valid replacements.
        
        Args:
            topics (list): A list of strings that are to be used as Kafka topics."""
        modified_topics = []
        for topic in topics:
            modified_topic = topic.replace(':', '_')
            modified_topics.append(modified_topic)
        return modified_topics
    
    def create_consumers(self):
        """create_consumers() creates a dictionary of kafka consumer objects, identified by their topic."""
        consumers = {}
        for topic in self.topics:
            consumers[topic] = KafkaConsumer(
            topic,
            bootstrap_servers=self.broker,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumers
            
