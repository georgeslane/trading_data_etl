from datetime import datetime
from utils.utils import DataBaseUtils
import mysql.connector

class DataBase:
    """DataBase is the base class for database initialisation.
    """
    def __init__(self, name: str, user: str, password: str):
        """Stores the database name and initailises the database by creating a conection to the database.

        Args:
            name (string): The name of the database.
            user (string): The name of the database user opening the connection.
            password (string): Required to authenticate the user.
        """
        self.name = name
        self.user = user
        self.password = password

    def connect(self):
        """Creates a connection to the database using provided arguments.
        """
        connection = mysql.connector.connect(
            host="localhost",
            user=self.user,
            password=self.password,
            database=self.name
        )
        return connection
    
    def airflow_connect(self):
        """Creates a connection to the database from airflow, using the service name 'mysql' as host instead of localhost.
        """
        connection = mysql.connector.connect(
            host="mysql",
            user=self.user,
            password=self.password,
            database=self.name
        )
        return connection


class Table(DataBase):
    """Table inherits from Database and is the base class for all tables.
    """
    def __init__(self, database_name: str, user: str, password: str, table_name: str):
        """Initialises Table by creating a database and storing the table name.
        
        Args:
            database_name (string): The name of the database being created.
            user (string): The name of the database user opening the connection.
            password (string): Required to authenticate the user.
            table_name (string): The name of the table being created.
        """
        super().__init__(name=database_name, user=user,password=password)
        self.table_name = table_name

    def create_table(self, create_statement: str):
        """Opens a connection to the database and creates a table using a passed query.
        
        Args:
            create_statement (string): The SQL statement used to create the table.
        """
        try:
            connection = self.connect()
        except:
            connection = self.airflow_connect()

        cursor = connection.cursor()
        cursor.execute(create_statement)
        connection.commit()
        cursor.close()
        connection.close()

    def insert_single_record(self, insert_statement: str, record: tuple):
        """Inserts a single record into a table.
        
        Args:
            insert_statement (string): The SQL string used to insert the record into the table.
            record (tuple): The record being inserted into the table.
        """
        try:
            connection = self.connect()
        except:
            connection = self.airflow_connect()

        cursor = connection.cursor()
        cursor.execute(insert_statement, record)
        connection.commit()
        cursor.close()
        connection.close()


class TradeConditionsReferenceTable(Table):
    """TradeConditionsReferenceTable inherits from table and stores the trade condition codes against their meanings 
    for use in the cleaned tables.
    """
    def __init__(self, database_name: str, user: str, password: str):
        """Initialises TradeConditionsReferenceTable by creating a reference table with trade code definitions.
        
        Args:
            database_name (string): The name of the database being created.
            user (string): The name of the database user opening the connection.
            password (string): Required to authenticate the user.
        """
        super().__init__(database_name=database_name, user=user, password=password, table_name="trade_conditions_reference_data")
        self.create_table_statement = """
            CREATE TABLE IF NOT EXISTS trade_conditions_reference_data (
            code INT PRIMARY KEY,
            name VARCHAR(63) NOT NULL
            )
            """
        
        self.create_table(create_statement=self.create_table_statement)

        self.trading_code_map = {
            1:"Regular",
            2:"Aquisition",
            3:"Average Price Trade",
            4:"Bunched",
            5:"Cash Sale",
            6:"Distribution",
            7:"Auomatic Execution",
            8:"Intermarket Sweep Order",
            9:"Bunched Sold",
            10:"Price Variation Trade",
            11:"Cap Election",
            12:"Odd Lot Trade",
            13:"Rule 127",
            14:"Rule 155",
            15:"Sold last",
            16:"Market Center Official Close",
            17:"Next day",
            18:"Market Center Opening Trade",
            19:"Opening Prints",
            20:"Market Center Official Open",
            21:"Prior Reference Price",
            22:"Seller",
            23:"Split Trade",
            24:"Form-T Trade",
            25:"Extended Hours (Sold Out of Sequence)",
            26:"Contingent Trade",
            27:"Stock Option Trade",
            28:"Cross Trade",
            29:"Yellow Flag",
            30:"Sold (Out of Sequence)",
            31:"Stopped Stock",
            32:"Derivatively Priced",
            33:"Market Center Re-opening Trade",
            34:"Re-opening Prints",
            35:"Market Center Closing Trade",
            36:"Closing Prints",
            37:"Qualified Contigent Trade",
            38:"Placeholder for 611 Exempt",
            39:"Corrected Consolidated Close",
            40:"Opened",
            41:"Trade Through Exempt (TTE)"
        }
        self.insert_statement = f"""INSERT IGNORE INTO {self.table_name}
            (code, name) VALUES (%s, %s)
            """
        for code in self.trading_code_map:
            record = (code, self.trading_code_map[code])
            self.insert_single_record(insert_statement=self.insert_statement, record=record)


class RawTable(Table):
    """RawTable inherits from Table and contains methods used to create, 
    update and query the initial layer of tables storing raw stock data directly from the websocket connection. 
    The table created has fields: id, trade_conditions, price, ticker, timestamp, volume.
    """
    def __init__(self, database_name: str, user: str, password: str, table_name: str):
        """Itialises the table if it does not already exist, using a specified create statement.
        
        Args:
            database_name (string): The name of the database being created.
            user (string): The name of the database user opening the connection.
            password (string): Required to authenticate the user.
            table_name (string): The name of the table being created, with format 'raw_<stock_symbol>'.
        """
        super().__init__(database_name=database_name, user=user, password=password, table_name=table_name)

        self.create_table_statement = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            trade_codes VARCHAR(127),
            price FLOAT,
            ticker VARCHAR(31) NOT NULL,
            unix_timestamp BIGINT,
            volume FLOAT
            )
            """
        
        self.insert_record_statement = f"""
            INSERT INTO {self.table_name} 
            (trade_codes, price, ticker, unix_timestamp, volume) 
            VALUES (%s, %s, %s, %s, %s)
            """
        
        self.create_table(self.create_table_statement)

    def insert_raw_stock_record(self, record: dict):
        """Inserts a new record into the raw table.
        
        Args:
            record (dictionary): The stock trade record being inserted into the table.
        """
        insertable_record = DataBaseUtils.get_record_from_consumer_message(record)
        self.insert_single_record(insert_statement=self.insert_record_statement, record=insertable_record)

    
class CleanedTable(Table):
    """CleanedTable inherits from table and contains methods to create and insert cleaned stock data into a table."""
    def __init__(self, database_name: str, user: str, password: str, table_name: str):
        """Itialises the table if it does not already exist, using a specified create statement.
        
        Args:
            database_name (string): The name of the database being created.
            user (string): The name of the database user opening the connection.
            password (string): Required to authenticate the user.
            table_name (string): The name of the table being created, with format 'cleaned_<stock_symbol>'.
        """
        super().__init__(database_name=database_name, user=user, password=password, table_name=table_name)
        self.create_table_statement = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
            id BIGINT PRIMARY KEY,
            trade_conditions JSON,
            price FLOAT,
            ticker VARCHAR(31) NOT NULL,
            timestamp DATETIME,
            volume FLOAT
            )
            """

        self.create_table(create_statement=self.create_table_statement)
        

class AggregatedTable(Table):
    """AggregatedTable inherits from Table and contains methods to aggregate raw stock data and add new aggregated fields."""
    def __init__(self, database_name: str, user: str, password: str, table_name: str):
        """Itialises the table if it does not already exist, using a specified create statement.
        
        Args:
            database_name (string): The name of the database being created.
            user (string): The name of the database user opening the connection.
            password (string): Required to authenticate the user.
            table_name (string): The name of the table being created, with format 'aggregated_<stock_symbol>'.
        """
        super().__init__(database_name=database_name, user=user, password=password, table_name=table_name)
        self.create_table_statement = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
            aggregated_id BIGINT AUTO_INCREMENT,
            start_id BIGINT NOT NULL,
            aggregation_period INT NOT NULL,
            ticker VARCHAR(31) NOT NULL,
            start_timestamp DATETIME,
            trade_conditions JSON,
            number_of_trades INT,
            total_volume FLOAT,
            average_volume FLOAT,
            average_price FLOAT,
            maximum_price FLOAT,
            minimum_price FLOAT,
            opening_price FLOAT,
            closing_price FLOAT,
            PRIMARY KEY (aggregated_id, aggregation_period)
            )
            """
        
        self.create_table(create_statement=self.create_table_statement)


class AccessTable(Table):
    """AccessTable inherits from Table and contains methods to union all aggregated stock data into one access layer table."""
    def __init__(self, database_name: str, user: str, password: str):
        """Itialises the table if it does not already exist, using a specified create statement.
        
        Args:
            database_name (string): The name of the database being created.
            user (string): The name of the database user opening the connection.
            password (string): Required to authenticate the user.
        """
        super().__init__(database_name=database_name, user=user, password=password, table_name="access_layer_stock_table")

        self.create_table_statement = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
            aggregated_id BIGINT,
            start_id BIGINT NOT NULL,
            aggregation_period INT NOT NULL,
            ticker VARCHAR(31) NOT NULL,
            start_timestamp DATETIME,
            trade_conditions JSON,
            number_of_trades INT,
            total_volume FLOAT,
            average_volume FLOAT,
            average_price FLOAT,
            maximum_price FLOAT,
            minimum_price FLOAT,
            opening_price FLOAT,
            closing_price FLOAT,
            PRIMARY KEY (aggregated_id, aggregation_period, ticker)
            )
            """
        
        self.create_table(create_statement=self.create_table_statement)
