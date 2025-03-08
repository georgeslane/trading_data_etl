import websocket
import json
from websocket._app import WebSocketApp
from kafka import KafkaConsumer, KafkaProducer
import concurrent.futures
from utils.utils import KafkaUtils, DataBaseUtils
from database.database import RawTable


class WebSocketConnection:
    """WebSocketConnection contains methods used to connect to and request data from a websocket connection. 
    It also implements a connection to a Kafka producer, which publishes data recieved by the websocket connection.
    """
    def __init__(self, api_key: str, ticker_symbols: list, producer: object):
        """Initialises the websocket connection by storing data used by the connection.
        
        Args:
            api_key (string): The API key used to connect to the desired websocket.
            ticker_symbols (list): A list of stock symbols that the websocket connection should subscribe to.
            producer (object): A Kafka producer object that published the recieved websocket data.
        """
        self.api_key = api_key
        self.ticker_symbols = ticker_symbols
        self.producer = producer

    def on_message(self, ws: object, message: str):
        """Specifies the action on recieveing a message (the data is pushed to the kafka producer to be published).
        
        Args:
            ws (object): The WebSocketApp object.
            message (string): The message recieved by the websocket connection
        """
        json_message = json.loads(message)
        self.producer.push_data(data=json_message)

    def on_error(self, ws, error):
        """Specifies the error mesaage to print in the case of a connection error.
        
        Args:
            ws (object): The WebSocketApp object.
            error: The error message recieved.
        """
        print("WebSocket Error:", error)

    def on_close(self, ws: object, close_status_code: str, close_msg: str):
        """Outputs a print statement with closing ststus and closing message when the websocket is closed.
        
        Args:
            ws (object): The WebSocketApp object.
            close_status_code (string): The status code recieved upon closing.
            close_msg (string): The message recieved from the websocket connection upon closing.
        """
        print(f"WebSocket Closed with code {close_status_code} and message {close_msg}")

    def on_open(self, ws: object):
        """Creates a number of subscriptions based on self.ticker_symbols and sends these subscriptions to the websocket.
        
        Args: 
            ws (object): The WebSocketApp object.
        """
        subscriptions = []
        for symbol in self.ticker_symbols:
            subscriptions.append(f'{{"type":"subscribe","symbol":"{symbol}"}}')
        
        for item in subscriptions:
            ws.send(item)

    def start_websocket(self):
        """Starts the websocket connection by initialising a WebSocketApp object 
        and mapping attributes to the functions defined above.
        """
        ws = WebSocketApp(
            f"wss://ws.finnhub.io?token={self.api_key}",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.on_open = self.on_open
        ws.run_forever()


class Producer(KafkaProducer):
    """Producer contains a method to publish returned data to a Kafka topic based on its stock symbol. 
    The class inherits from the kafka.KafkaProducer base class
    """
    def __init__(self, broker: str, ticker_symbols: list):
        """Initialises Producer by storing the broker server, 
        getting topic names, and creating a KafkaProducer object which published to the broker.
        
        Args:
            broker (string): The port that is setup to run the kafka broker.
            ticker_symbols (list): A list of stock symbols that the websocket connection is subscribed to.
        """
        self.broker = broker
        self.topics_dictionary = KafkaUtils.update_topic_names(topics=ticker_symbols)
        super().__init__(
            bootstrap_servers=self.broker, 
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def push_data(self, data: dict):
        """Gets each individual trade from the data passed as an argument. 
        Sends each trade to the relevant topic using the ticker_symbol: topic mapping dictionary.
        
        data (dictionary): A dictionary containing trading data sent by the websocket contaction. In the following format:
        {'data': 
            [
                {
                    'c': Tradeconditions,
                    'p':Price,
                    't': Timestamp,
                    'v': Volume,
                    's': Symbol
                }, 
                {   
                    'c': Tradeconditions,
                    'p':Price,
                    't': Timestamp,
                    'v': Volume,
                    's': Symbol
                }
            ],
        'type':'trade'
        }
        """
        try:
            data_list = data['data']
            for item in data_list:
                self.send(topic=self.topics_dictionary[item['s']], value=item)
        except KeyError:
            pass


class Consumer():
    """Consumer sets up several consumers in parrallel to subscribe to each topic and publish recieved data to a database.
    """
    def __init__(self, ticker_symbols: list, broker: str, database_name: str):
        """Initialises Consumer by storing the broker, the database name and the tables in the database.
        
        Args:
            ticker_symbols (list): The list of symbols the websocket connection is subscribed to.
            broker(string): The port that is running the kafka broker.
            database_name (string): The name of the database that will be used to store the websocket data.
        """
        self.broker = broker
        self.database_name = database_name
        self.topics_dictionary = KafkaUtils.update_topic_names(topics=ticker_symbols)
        self.raw_tables = DataBaseUtils.get_raw_tables(
            topics=ticker_symbols, 
            table_class=RawTable, 
            database_name=self.database_name
        )

    def setup_consumers(self, topic: str):
        """Creates a consumer that listens to a specified topic and 
        inserts any recieved data into a table that corresponds to that topic.
        
        Args:
            topic (string): The name of the ticker symbol used to determine the topic the consumer will subscribe to, 
            and the name of the table it will publish to.
        """
        consumer = KafkaConsumer(
            self.topics_dictionary[topic],
            bootstrap_servers=self.broker,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            self.raw_tables[topic].insert_record(message.value)

    def run_consumers(self):
        """Creates and runs consumers for all topics in parrallel threads."""
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.setup_consumers, topic) for topic in self.topics_dictionary]

