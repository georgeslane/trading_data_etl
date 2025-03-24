class Utils:
    """Utils contains methods used by the Kafka instance."""
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
            updated_topic = topic.lower()
            for char in replacement_dictionary:
                updated_topic = updated_topic.replace(char, replacement_dictionary[char])
            kafka_valid_topic_names[topic] = updated_topic

        return kafka_valid_topic_names
    
    def cursor_results_to_dict(cursor: list):
        """Convert query results into a list of dictionaries.
        
        Args:
            cursor (list): The results of a SQL query in the form of a list of tuples.
        """
        columns = [col[0] for col in cursor.description]  # Get column names
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    

class DataBaseUtils:
    """DataBaseUtils contains methods used by the MySQL database instance."""
    def __init__():
        """Placeholder initialisation function."""
        pass

    def get_kafka_tables(topics: list, raw_trades_table_class: object, raw_news_table_class:object, database_name: str, user: str, password: str):
        """Takes a list of stock symbols as input, 
        cleans each stock symbol of special characters and creates tables in the format 
        'raw_<cleaned_stock_symbol>' and 'news_<cleaned_stock_symbol>'. 
        Returns a dictionary in the format: {'stock_symbol': {'trades': RawTradeTable, 'news': RawNewsTable}}
        
        Args:
            topics (list): A list of stock symbols.
            raw_trades_table_class (object): The class used to create the raw stock trade tables.
            raw_news_table_class (object): The class used to create the raw stock news tables
            database_name (string): The name of the database where the tables are created.
            user (string): The account using the database.
            password (string): Required to authenticate the user.
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
            raw_tables_dictionary[topic]["trades"] = raw_trades_table_class(
                database_name = database_name,
                user=user,
                password=password, 
                table_name = f'raw_{updated_topic}')
            raw_tables_dictionary[topic]["news"] = raw_news_table_class(
                database_name = database_name,
                user=user,
                password=password, 
                table_name = f'news_{updated_topic}')
        return raw_tables_dictionary
    
    def get_record_from_consumer_message(consumer_message: dict):
        """Takes a dictionary containing a market trade or news and returns a tuple ready for insertion into a database.
        
        Args:
            consumer_message (dictionary): The dictionary containing data on the trade.
        """
        if consumer_message["type"] == "news":
            source_id = int(consumer_message["id"])
            unix_timestamp = int(consumer_message["datetime"])
            headline = str(consumer_message["headline"])
            ticker = str(consumer_message["related"])
            source_site = str(consumer_message["source"])
            summary = str(consumer_message["summary"])
            record = (source_id, unix_timestamp, headline, ticker, source_site, summary)
        elif consumer_message["type"] == "trade":
            conditions = str(consumer_message['c'])
            price = float(consumer_message['p'])
            ticker = str(consumer_message['s'])
            timestamp = int(consumer_message['t'])
            volume = float(consumer_message['v'])
            record = (conditions, price, ticker, timestamp, volume)
        
        return record