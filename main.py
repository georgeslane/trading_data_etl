from kafka_pipeline.apache_kafka_producer import WebSocketConnection, KafkaServer
from settings import FINHUB_API_KEY

def main():
    # Kafka Config
    KAFKA_BROKER = 'localhost:29092'
    KAFKA_TOPIC = 'financial_data'
    ticker_symbols = ['AAPL', 'AMZN']

    KafkaInstance = KafkaServer(broker=KAFKA_BROKER, topic=KAFKA_TOPIC)

    FinnHubConnection = WebSocketConnection(kafka_producer = KafkaInstance.producer,
                                            api_key = FINHUB_API_KEY,
                                            topic=KAFKA_TOPIC,
                                            ticker_symbols = ticker_symbols)
    

    KafkaInstance.get_messages()

if __name__ == "__main__":
    main()