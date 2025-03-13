from apache_kafka.apache_kafka_producer import WebSocketConnection, Producer, Consumer
from config.settings import FINHUB_API_KEY, KAFKA_DATABASE_PASSWORD, KAFKA_DATABASE_USER
import threading

def main():
    KAFKA_BROKER = 'localhost:29092'
    TICKER_SYMBOLS = ["AMZN","IC MARKETS:1", "BINANCE:BTCUSDT","AAPL"]


    ProducerInstance = Producer(
        broker=KAFKA_BROKER,
        ticker_symbols=TICKER_SYMBOLS
    )

    ConsumerInstance = Consumer(
        ticker_symbols=TICKER_SYMBOLS,
        broker=KAFKA_BROKER,
        database_name="FINANCE_DATABASE",
        password=KAFKA_DATABASE_PASSWORD,
        user=KAFKA_DATABASE_USER
    )

    FinnHubConnection = WebSocketConnection(
        api_key=FINHUB_API_KEY,
        ticker_symbols=TICKER_SYMBOLS,
        producer=ProducerInstance
    )

    threads = {
        "producer_thread": threading.Thread(target=FinnHubConnection.start_websocket),
        "consumer_thread": threading.Thread(target=ConsumerInstance.run_consumers)
    }

    for thread in threads:
        threads[thread].start()
    for thread in threads:
        threads[thread].join()

if __name__ == "__main__":  
    main()