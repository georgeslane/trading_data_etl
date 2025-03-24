from utils.utils import Utils
from datetime import datetime
from websocket._app import WebSocketApp
import json


class WebSocketConnection:
    """WebSocketConnection contains methods used to connect to and request data from a websocket connection. 
    It also implements a connection to a Kafka producer, which publishes data recieved by the websocket connection.
    """
    def __init__(self, endpoint: str, ticker_symbols: list, producer: object):
        """Initialises the websocket connection by storing data used by the connection.
        
        Args:
            api_key (string): The API key used to connect to the desired websocket.
            endpoint (string): The endpoint of the websocket.
            ticker_symbols (list): A list of stock symbols that the websocket connection should subscribe to.
            producer (object): A Kafka producer object that published the recieved websocket data.
        """
        self.endpoint = endpoint
        self.ticker_symbols = ticker_symbols
        self.producer = producer
        self.connection_string = ''
        self.connection_site = ''

    def on_message(self, ws: object, message):
        """Specifies the action on recieveing a message (the data is pushed to the kafka producer to be published).
        
        Args:
            ws (object): The WebSocketApp object.
            message (string): The message recieved by the websocket connection
        """
        json_message = json.loads(message)
        self.producer.push_trade_data(data=json_message, 
                                      source=self.connection_site)

    def on_error(self, ws, error):
        """Specifies the error mesaage to print in the case of a connection error.
        
        Args:
            ws (object): The WebSocketApp object.
            error: The error message recieved.
        """
        print("WebSocket Error:", error)

    def on_close(self, ws: object, close_status_code, close_msg):
        """Outputs a print statement with closing ststus and closing message when the websocket is closed.
        
        Args:
            ws (object): The WebSocketApp object.
            close_status_code (int): The status code recieved upon closing.
            close_msg (string): The message recieved from the websocket connection upon closing.
        """
        status = close_status_code if close_status_code is not None else "Unknown"
        message = close_msg if close_msg is not None else "No message"
        print(f"WebSocket Closed with code {status} and message {message}")

    def start_websocket(self):
        """Starts the websocket connection by initialising a WebSocketApp object 
        and mapping attributes to the functions defined above.
        """
        ws = WebSocketApp(
            f"{self.connection_string}",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.on_open = self.on_open
        ws.run_forever()


class FinnhubWebsocket(WebSocketConnection):
    """Creates a websocket connection to a Finnhub websocket.
    """
    def __init__(self, api_key: str, endpoint: str, ticker_symbols: list, producer: object):
        """Initialises the websocket connection by storing data used by the connection.
        
        Args:
            api_key (string): The API key used to connect to the finnhub websocket.
            endpoint (string): The endpoint of the websocket.
            ticker_symbols (list): A list of stock symbols that the websocket connection should subscribe to.
            producer (object): A Kafka producer object that published the recieved websocket data.
        """
        super().__init__(endpoint=endpoint,
                         ticker_symbols=ticker_symbols,
                         producer=producer)
        self.api_key = api_key
        self.connection_string = f"{self.endpoint}?token={self.api_key}"
        self.connection_site = "Finnhub"
        
    def on_open(self, ws: object):
        """Creates a number of subscriptions based on self.ticker_symbols 
        and sends these subscriptions to the websocket.
        
        Args: 
            ws (object): The WebSocketApp object.
        """
        subscriptions = []
        for symbol in self.ticker_symbols[self.connection_site]:
            subscriptions.append(f'{{"type":"subscribe","symbol":"{symbol}"}}')
        
        for item in subscriptions:
            ws.send(item)


class CoinbaseWebsocket(WebSocketConnection):
    """Creates a websocket connection to a coinbase websocket.
    """
    def __init__(self, endpoint: str, ticker_symbols: list, producer: object):
        """Initialises the websocket connection by storing data used by the connection.
        
        Args:
            api_key (string): The API key used to connect to the finnhub websocket.
            endpoint (string): The endpoint of the websocket.
            ticker_symbols (list): A list of stock symbols that the websocket connection should subscribe to.
            producer (object): A Kafka producer object that published the recieved websocket data.
        """
        super().__init__(endpoint=endpoint,
                         ticker_symbols=ticker_symbols,
                         producer=producer)
        self.connection_string = self.endpoint
        self.connection_site = "Coinbase"
        
    def on_open(self, ws:object):
        """Subscribes to selected ticker symbols.
        
        Args:
            ws (object): The WebSocketApp object.
        """
        subscribe_message = {
        "type": "subscribe",
        "channels": [
                {
                    "name": "ticker", 
                    "product_ids": self.ticker_symbols[self.connection_site]
                }
            ]
        }
        ws.send(json.dumps(subscribe_message))

    