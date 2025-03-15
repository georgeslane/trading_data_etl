import requests
from utils.utils import Utils
from datetime import datetime
from websocket._app import WebSocketApp
import json


class ApiConnection:
    """Creates a connection to a news api and sends recieved responses to a Kafka producer.
    """

    def __init__(self, api_key: str, ticker_list: list, producer: object, url: str):
        """Initialises the ApiConnection class.
        
        Args:
            api_key (string): Used to authenticate the connection.
            ticker_list (list): Contains the stocks to request data on.
            producer (object): The Kafka producer to send data to.
            url (string): The API endpoint to connect to.
        """
        self.api_key = api_key
        self.ticker_topic_mapping = Utils.update_topic_names(topics=ticker_list)
        self.producer = producer
        self.url = url
        self.date = datetime.now().strftime("%Y-%m-%d")

    def get_data(self):
        while True:
            for symbol in self.ticker_topic_mapping:
                full_url = f"{self.url}symbol={symbol}&from={self.date}&to={self.date}&token={self.api_key}"
                response = requests.get(url=full_url)

                if response.status_code == 200:
                    data = response.json()
                    self.producer.push_news_data(data=data)
                else:
                    continue

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
        self.producer.push_trade_data(data=json_message)

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
