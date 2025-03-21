from real_time_data_classes.data_connections import CoinbaseWebsocket, FinnhubWebsocket
from real_time_data_classes.apache_kafka import Producer, Admin
from config.settings import FINHUB_API_KEY

ticker_dictionary = {
    "Finnhub": ["AAPL", "AMZN"],
    "Coinbase": ["BTC-USD", "ETH-USD"]
}

AdminInstance = Admin(broker='localhost:29092', 
                      client_id='topics_manager')

AdminInstance.setup_topics(tickers_dictionary=ticker_dictionary)


PDInstance = Producer(broker='localhost:29092')


CBInstance = CoinbaseWebsocket(endpoint="wss://ws-feed.exchange.coinbase.com",
                             ticker_symbols = ticker_dictionary,
                             producer=PDInstance)

FNInstance = FinnhubWebsocket(api_key=FINHUB_API_KEY,
                              endpoint="wss://ws.finnhub.io",
                              ticker_symbols=ticker_dictionary,
                              producer=PDInstance)

# CBInstance.start_websocket()
FNInstance.start_websocket()



