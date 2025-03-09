class KafkaUtils:
    """KafkaUtils contains methods used by the Kafka instance."""
    def __init__():
        """Placeholder initialisation function."""
        pass

    def update_topic_names(topics: list):
        """Takes as input a list of ticker symbols. 
        Removes any special characters to create corresponsing kafka topics and returns a dictionary in the form: 
        {'ticker_symbol':'kafka_topic'}
        
        Args:
            topics (list): A list of stock symbols.
        """
        kafka_valid_topic_names = {}
        replacement_dictionary = {
            ':':'_',
            ' ':'_'
        }
        for topic in set(topics):
            updated_topic = topic
            for char in replacement_dictionary:
                updated_topic = updated_topic.replace(char, replacement_dictionary[char])
            kafka_valid_topic_names[topic] = updated_topic

        return kafka_valid_topic_names
    

class DataBaseUtils:
    def __init__():
        """Placeholder initialisation function."""
        pass

    def get_raw_tables(topics: list, table_class: object, database_name: str):
        """Takes a list of stock symbols as input, 
        cleans each stock symbol of special characters and creates a table in the format 'raw_<cleaned_stock_symbol>'. 
        Returns a dictionary in the format: {'stock_symbol':Table object}
        
        Args:
            topics (list): A list of stock symbols.
            table_class (object): The class used to create the raw stock tables.
            database_name (string): The name of the database where the tables are created.
        """
        raw_tables_dictionary = {}
        replacement_dictionary = {
            ':':'_',
            ' ':'_'
        }
        for topic in set(topics):
            updated_topic = topic.lower()
            for char in replacement_dictionary:
                updated_topic = updated_topic.replace(char, replacement_dictionary[char])
            raw_tables_dictionary[topic] = table_class(
                database_name = database_name, 
                table_name = f'raw_{updated_topic}')
        return raw_tables_dictionary
    
    def get_record_from_consumer_message(consumer_message: dict):
        """Takes a dictionary containing a market trade and returns a tuple ready for insertion into a database.
        
        Args:
            consumer_message (dictionary): The dictionary containing data on the trade.
        """
        conditions = str(consumer_message['c'])
        price = float(consumer_message['p'])
        ticker = str(consumer_message['s'])
        timestamp = int(consumer_message['t'])
        volume = float(consumer_message['v'])
        record = (conditions, price, ticker, timestamp, volume)
        return record