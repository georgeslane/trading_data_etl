from real_time_data_classes.apache_kafka import Producer, Admin
from real_time_data_classes.data_connections import CoinbaseWebsocket, FinnhubWebsocket
from config.settings import (FINHUB_API_KEY, 
                             KAFKA_DATABASE_PASSWORD, 
                             KAFKA_DATABASE_USER, 
                             STOCK_LIST, 
                             DATABASE_NAME,
                             KAFKA_BROKER,
                             BACKUP_KAFKA_BROKER)
import threading
import os

def main():
    # Runs the docker command to start the kafka, airflow and mysql containers.
    os.system("docker-compose up -d")

    # Initialises a kafka admin object.
    AdminInstance = Admin(broker=KAFKA_BROKER, 
                      client_id='topics_manager')

    # Creates a kafka producer.
    ProducerInstance = Producer(
        broker=KAFKA_BROKER
    )

    # Creates coinbase and finnhub websocket connections.
    CoinbaseInstance = CoinbaseWebsocket(endpoint="wss://ws-feed.exchange.coinbase.com",
                             ticker_symbols = STOCK_LIST,
                             producer=ProducerInstance)

    FinnhubInstance = FinnhubWebsocket(api_key=FINHUB_API_KEY,
                              endpoint="wss://ws.finnhub.io",
                              ticker_symbols=STOCK_LIST,
                              producer=ProducerInstance)
    
    AdminInstance.setup_topics(tickers_dictionary=STOCK_LIST)

    # Creates and starts parrallel threads for the websockets being subscribed to.
    threads = {
        "finnhub_thread": threading.Thread(target=FinnhubInstance.start_websocket),
        "coinbase_thread": threading.Thread(target=CoinbaseInstance.start_websocket)
    }

    for thread in threads:
        threads[thread].start()
    for thread in threads:
        threads[thread].join()

if __name__ == "__main__":  
    main()