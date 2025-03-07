import websocket
import json
from websocket._app import WebSocketApp
from kafka import KafkaConsumer, KafkaProducer
import concurrent.futures
from utils.utils import KafkaUtils, DataBaseUtils
from database.database import RawTable
import threading


class WebSocketConnection:
    def __init__(self, api_key: str, ticker_symbols: list, producer: object):
        self.api_key = api_key
        self.ticker_symbols = ticker_symbols
        self.producer = producer

    def on_message(self, ws, message):
        json_message = json.loads(message)
        self.producer.push_data(data=json_message)

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


class Producer(KafkaProducer):
    def __init__(self, broker: str, ticker_symbols: list):
        self.broker = broker
        self.topics_dictionary = KafkaUtils.update_topic_names(topics=ticker_symbols)
        super().__init__(
            bootstrap_servers=self.broker, 
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def push_data(self, data: dict):
        try:
            data_list = data['data']
            for item in data_list:
                self.send(topic="BINANCE_BTCUSDT", value=item)
        except KeyError:
            pass


class Consumer():
    def __init__(self, ticker_symbols: list, broker: str, database_name: str):
        self.broker = broker
        self.database_name = database_name
        self.topics_dictionary = KafkaUtils.update_topic_names(topics=ticker_symbols)
        self.raw_tables = DataBaseUtils.get_raw_tables(
            topics=ticker_symbols, 
            table_class=RawTable, 
            database_name=self.database_name
        )

    def setup_consumers(self, topic: str):
        consumer = KafkaConsumer(
            self.topics_dictionary[topic],
            bootstrap_servers=self.broker,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            self.raw_tables[topic].insert_record(message.value)

    def run_consumers(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.setup_consumers, topic) for topic in self.topics_dictionary]









# class KafkaServer:
#     def __init__(self, broker, topics, tables_dictionary):
#         self.topics = self.get_topics(topics)
#         self.broker = broker
#         self.tables_dictionary = tables_dictionary
#         self.consumers = self.create_consumers()
#         self.producer = KafkaProducer(
#             bootstrap_servers=self.broker,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8')
#         )

#     def insert_data_into_raw_tables(self, topic):
#         """insert_data_into_raw_tables() inserts records from each consumer object in self.consumers into a corresponsing table.
        
#         Args:
#             tables_dictionary (dictionary): Defines a dictionary of tables, where each table is identified by a Kafka topic."""
#         while True:
#             for message in self.consumers[topic]:
#                 self.tables_dictionary[f'raw_{topic}'].insert_record(message.value)

#     def get_topics(self, topics):
#         """get_topics() replaces invalid chars in a list of topics with valid replacements.
        
#         Args:
#             topics (list): A list of strings that are to be used as Kafka topics."""
#         modified_topics = []
#         for topic in topics:
#             modified_topic = topic.replace(':', '_')
#             modified_topics.append(modified_topic)
#         return modified_topics
    
#     def create_consumers(self):
#         """create_consumers() creates a dictionary of kafka consumer objects, identified by their topic."""
#         consumers = {}
#         for topic in self.topics:
#             consumers[topic] = KafkaConsumer(
#             topic,
#             bootstrap_servers=self.broker,
#             auto_offset_reset='earliest',
#             value_deserializer=lambda x: json.loads(x.decode('utf-8'))
#         )
#         return consumers
    
#     def simultaneously_insert_data(self):
#         with concurrent.futures.ThreadPoolExecutor() as executor:
#             while True:
#                 executor.map(self.insert_data_into_raw_tables, self.topics)
            
