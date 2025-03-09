from apache_kafka.apache_kafka_producer import WebSocketConnection, Producer, Consumer
from settings import FINHUB_API_KEY
import json
from kafka import KafkaConsumer
import threading

def main():
    # Kafka Config
    KAFKA_BROKER = 'localhost:29092'
    TICKER_SYMBOLS = ["IC MARKETS:1", "BINANCE:BTCUSDT","AAPL", "AMZN"]



    ProducerInstance = Producer(
        broker=KAFKA_BROKER,
        ticker_symbols=TICKER_SYMBOLS
    )

    ConsumerInstance = Consumer(
        ticker_symbols=TICKER_SYMBOLS,
        broker=KAFKA_BROKER,
        database_name="FINANCE_DATABASE"
    )

    FinnHubConnection = WebSocketConnection(
        api_key=FINHUB_API_KEY,
        ticker_symbols=TICKER_SYMBOLS,
        producer=ProducerInstance
    )

    threads = {
        "consumer_thread": threading.Thread(target=ConsumerInstance.run_consumers),
        "producer_thread": threading.Thread(target=FinnHubConnection.start_websocket)
    }

    for thread in threads:
        threads[thread].start()
    for thread in threads:
        threads[thread].join()

if __name__ == "__main__":  
    main()