from apache_kafka.apache_kafka_producer import WebSocketConnection, KafkaServer
from database.database import RawTable
from settings import FINHUB_API_KEY

def main():
    # Kafka Config
    KAFKA_BROKER = 'localhost:29092'
    KAFKA_TOPIC = "BINANCE_BTCUSDT"
    ticker_symbols = ["AAPL", "AMZN"]

    KafkaInstance = KafkaServer(broker=KAFKA_BROKER, topics=ticker_symbols)

    FinnHubConnection = WebSocketConnection(kafka_producer = KafkaInstance.producer,
                                            api_key = FINHUB_API_KEY,
                                            ticker_symbols=ticker_symbols)

    FinnHubConnection.start_websocket()

    tables = {}
    for stock in ticker_symbols:
        modified_stock = stock.replace(':', '_')
        tables[f'raw_{modified_stock}'] = RawTable(database_name = 'FinanceDB', table_name = f'raw_{modified_stock}')

    KafkaInstance.insert_data_into_raw_tables(tables_dictionary=tables)

if __name__ == "__main__":  
    main()

