from airflow.decorators import task
import os
import sys
import json
import ast
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from database.database import TradeConditionsReferenceTable, CleanedTable, AggregatedTable, AccessTable
from utils.utils import Utils

tickers = ["BINANCE:BTCUSDT", "AMZN"]

@task
def initialise_tables(ticker_symbols: list, database_name: str, user: str, password: str):
    """Creates raw, cleaned, aggregated, unioned and reference tables for the trading data and saves references to them in a dictionary.
    
    Args:
        ticker_symbols (list): A list of stock symbols being subscribed to.
        database_name (string): The database to create tables in.
        user (string): The user creating these tables.
        password (string): Used to authenticate the user.
    """

    ticker_dictionary = Utils.update_topic_names(topics=ticker_symbols)
    table_dictionary = {
        "cleaned_tables":[],
        "aggregated_tables":[],
    }
    for symbol in ticker_dictionary:
        table_dictionary["cleaned_tables"].append(CleanedTable(
            database_name=database_name, 
            user=user, 
            password=password, 
            table_name=f"cleaned_{ticker_dictionary[symbol]}")
            )
        
        table_dictionary["aggregated_tables"].append(AggregatedTable(
            database_name=database_name, 
            user=user, 
            password=password, 
            table_name=f"aggregated_{ticker_dictionary[symbol]}")
            )
        
    table_dictionary["access_table"] = AccessTable(database_name=database_name, 
                                                   user=user, 
                                                   password=password
                                                   )
    
    table_dictionary["trade_conditions_reference_table"] = TradeConditionsReferenceTable(database_name=database_name, 
                                                                                         user=user, 
                                                                                         password=password
                                                                                         )
    
@task
def get_table_names(tickers: list):
    """Gets the cleaned dictionary of table name suffixes from the list of stock sysmbols.
    
    Args:
        tickers (list): A list of stock symbols.
    """
    results = Utils.update_topic_names(topics=tickers)
    return results

@task
def clean_raw_data(raw_query_result: list, reference_codes: list):
    """Cleans the raw data returned from raw tables by getting the correct trade code definitions 
    and converting the timestamp into a DataTime datatype.
    
    Args:
        raw_query_result (list): The results of querying the most recent additions to the raw_stock tables.
        reference_codes (list): Maps the trade codes to their definitions.
    """
    output_string = ""
    reference_dictionary = {}
    for record in reference_codes:
        modified_dict = {str(record["code"]):record["name"]}
        reference_dictionary = {**reference_dictionary, **modified_dict}
    print(reference_dictionary)
    for record in raw_query_result:
        codes_list = ast.literal_eval(record["trade_codes"])
        trade_conditions = {}
        try:
            for number in codes_list:
                updated_number = str(number)
                trade_conditions[updated_number] = reference_dictionary[number]
        except TypeError:
            trade_conditions = {}

        timestamp = datetime.fromtimestamp(int(record["unix_timestamp"])/1000).strftime("%Y-%m-%d %H:%M:%S")
        output_record = (record["id"], json.dumps(trade_conditions), record["price"], record["ticker"], timestamp, record["volume"])
        output_string = f"{output_string}{str(output_record)}, "
    
    output_string = f"VALUES {output_string}"
    if output_string == "VALUES ":
        output_string = "SELECT '0', NULL, NULL, 'NULL', NULL, NULL WHERE FALSE;"
    
    return output_string.rstrip(", ")

    
        