class KafkaUtils:
    """KafkaUtils contains methods used by the Kafka instance."""
    def __init__(self):
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
    def __init__(self):
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