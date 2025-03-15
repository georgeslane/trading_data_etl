import json
from kafka import KafkaConsumer, KafkaProducer
import concurrent.futures
from utils.utils import Utils, DataBaseUtils
from database.database import RawTable, NewsTable

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
        self.topics_dictionary = Utils.update_topic_names(topics=ticker_symbols)
        super().__init__(
            bootstrap_servers=self.broker, 
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def push_trade_data(self, data: dict):
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
                item["type"] = "trade"
                self.send(topic=self.topics_dictionary[item['s']], value=item)
        except KeyError:
            pass

    def push_news_data(self, data:list):
        """Called when news data on the ticker watchlist is recived. Parses the data and then publishes.
        
        Args: 
            data (list): News data. Takes the form of a list of dictionaries.
        """
        for item in data:
            item["type"] = "news"
            self.send(topic=self.topics_dictionary[item['related']], value=item)


class Consumer():
    """Consumer sets up several consumers in parrallel to subscribe to each topic and publish recieved data to a database.
    """
    def __init__(self, ticker_symbols: list, broker: str, database_name: str, user: str, password: str):
        """Initialises Consumer by storing the broker, the database name and the tables in the database.
        
        Args:
            ticker_symbols (list): The list of symbols the websocket connection is subscribed to.
            broker(string): The port that is running the kafka broker.
            database_name (string): The name of the database that will be used to store the websocket data.
            user (string): The database user.
            password (string): Used to authenticate the database user.
        """
        self.broker = broker
        self.database_name = database_name
        self.user = user
        self.password = password
        self.topics_dictionary = Utils.update_topic_names(topics=ticker_symbols)
        self.raw_tables = DataBaseUtils.get_kafka_tables(
            topics=ticker_symbols, 
            raw_trades_table_class=RawTable, 
            raw_news_table_class=NewsTable,
            database_name=self.database_name,
            user=self.user,
            password=self.password
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
        while True:
            for message in consumer:
                if message["type"] == "trade":
                    self.raw_tables[topic]["trades"].insert_stock_record(message.value)
                elif message["type"] == "news":
                    self.raw_tables[topic]["news"].insert_stock_record(message.value)

    def run_consumers(self):
        """Creates and runs consumers for all topics in parrallel threads."""
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.setup_consumers, topic) for topic in self.topics_dictionary]

