from real_time_data_classes.apache_kafka import Producer, Consumer
from real_time_data_classes.data_connections import WebSocketConnection, ApiConnection
from config.settings import FINHUB_API_KEY, KAFKA_DATABASE_PASSWORD, KAFKA_DATABASE_USER, STOCK_LIST, DATABASE_NAME
import threading
import os

def main():
    # Runs the docker command to start the kafka, airflow and mysql containers.
    os.system("docker-compose up -d")
    KAFKA_BROKER = 'localhost:29092'
    TICKER_SYMBOLS = STOCK_LIST

    # Creates a kafka producer
    ProducerInstance = Producer(
        broker=KAFKA_BROKER,
        ticker_symbols=TICKER_SYMBOLS
    )

    # Creates a kafka consumer for each stock.
    ConsumerInstance = Consumer(
        ticker_symbols=TICKER_SYMBOLS,
        broker=KAFKA_BROKER,
        database_name=DATABASE_NAME,
        password=KAFKA_DATABASE_PASSWORD,
        user=KAFKA_DATABASE_USER
    )

    # Creates a websocker connection that listens for specific data.
    FinnHubTradesConnection = WebSocketConnection(
        api_key=FINHUB_API_KEY,
        ticker_symbols=TICKER_SYMBOLS,
        producer=ProducerInstance
    )

    FinnHubNewsConnection = ApiConnection(
        api_key=FINHUB_API_KEY,
        ticker_list=TICKER_SYMBOLS,
        producer=ProducerInstance,
        url="https://finnhub.io/api/v1/company-news?"
    )

    # Starts two threads so the consumers and producer can run in parrallel.
    threads = {
        "websocket_thread": threading.Thread(target=FinnHubTradesConnection.start_websocket),
        "api_thread": threading.Thread(target=FinnHubNewsConnection.get_data),
        "consumer_thread": threading.Thread(target=ConsumerInstance.run_consumers)
    }

    for thread in threads:
        threads[thread].start()
    for thread in threads:
        threads[thread].join()

if __name__ == "__main__":  
    main()