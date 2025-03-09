from datetime import datetime
import mysql.connector

class DataBase:
    """DataBase is the base class for database initialisation.
    """
    def __init__(self, name: str):
        """Stores the database name and initailises the database by creating a conection to the database.

        Args:
            name (string): The name of the database.
        """
        self.name = name

    def connect(self):
        connection = mysql.connector.connect(
            host="localhost",
            user="kafka_consumer",
            password="password",
            database="FINANCE_DATABASE"
        )
        return connection


class Table(DataBase):
    """Table inherits from Database and is the base class for all tables.
    """
    def __init__(self, database_name: str, table_name: str):
        """Initialises Table by creating a database and storing the table name.
        
        Args:
            database_name (string): The name of the database being created.
            table_name (string): The name of the table being created.
        """
        super().__init__(database_name)
        self.table_name = table_name

    def create_table(self, create_statement: str):
        """Opens a connection to the database and creates a table using a passed query.
        
        Args:
            create_statement (string): The SQL statement used to create the table.
        """
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute(create_statement)
        connection.commit()
        cursor.close()
        connection.close()


class RawTable(Table):
    """RawTable inherits from Table and contains methods used to create, 
    update and query the initial layer of tables storing raw stock data directly from the websocket connection. 
    The table created has fields: id, trade_conditions, price, ticker, timestamp, volume.
    """
    def __init__(self, database_name: str, table_name: str):
        """Itialises the table if it does not already exist, using a specified create statement.
        
        Args:
            database_name (string): The name of the database being created.
            table_name (string): The name of the table being created, with format 'raw_<stock_symbol>'.
        """
        super().__init__(database_name, table_name)
        self.create_table_statement = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            trade_conditions VARCHAR(127),
            price FLOAT NOT NULL,
            ticker VARCHAR(31) NOT NULL,
            timestamp DATETIME NOT NULL,
            volume FLOAT
            )
            """
        self.create_table(self.create_table_statement)

    def insert_record(self, record: dict):
        """Inserts a new record into the raw table.
        
        Args:
            record (dictionary): The stock trade record being inserted into the table.
        """
        conditions = str(record['c'])
        price = float(record['p'])
        ticker = str(record['s'])
        timestamp = datetime.fromtimestamp(int(record['t']) / 1000)
        volume = float(record['v'])

        connection = self.connect()
        cursor = connection.cursor()
        query = f"INSERT INTO {self.table_name} (trade_conditions, price, ticker, timestamp, volume) VALUES (?, ?, ?, ?, ?)"
        cursor.execute(query, (conditions, price, ticker, timestamp, volume))
        cursor.close()
        connection.close()

