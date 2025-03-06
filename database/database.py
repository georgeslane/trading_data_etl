import sqlite3
from datetime import datetime

class DataBase:
    def __init__(self, name):
        self.name = name
        with sqlite3.connect(self.name) as self.connection:
            pass


class Table(DataBase):
    def __init__(self, database_name, table_name):
        super().__init__(database_name)
        self.table_name = table_name

    def create_table(self, create_statement):
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute(create_statement)
            self.connection.commit()


class RawTable(Table):
    def __init__(self, database_name, table_name):
        super().__init__(database_name, table_name)
        self.create_table_statement = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_conditions STRING,
            price FLOAT NOT NULL,
            ticker STRING NOT NULL,
            timestamp DATETIME NOT NULL,
            volume FLOAT
            )
            """
        self.create_table(self.create_table_statement)

    def insert_record(self, record):
        conditions = str(record['c'])
        price = float(record['p'])
        ticker = str(record['s'])
        timestamp = datetime.fromtimestamp(int(record['t']) / 1000)
        volume = float(record['v'])
        with self.connection:
            cursor = self.connection.cursor()
            query = f"INSERT INTO {self.table_name} (trade_conditions, price, ticker, timestamp, volume) VALUES (?, ?, ?, ?, ?)"
            cursor.execute(query, (conditions, price, ticker, timestamp, volume))

