from airflow.decorators import task
import pandas as pd
import requests
from datetime import datetime

@task
def aggregate_data(cleaned_trade_list: list):
    """Converts the list of trades to a dictionary. 
    Aggregates the data over selected periods. 
    Adds new aggregated attributes.
    
    Args:
        cleaned_trade_list (list): A list of cleaned trades from all sources.
    """

    trades_dataframe = pd.DataFrame(cleaned_trade_list)
    aggregated_trades_dataframe = trades_dataframe.groupby(["aggregation_period", "ticker"]).agg(
        max_price=("price", "max"),
        min_price=("price", "min"),
        opening_price=("price", lambda x: x.iloc[0]),
        closing_price=("price", lambda x: x.iloc[-1]),
        average_price=("price", "mean"),
        total_volume=("volume", "sum"),
        average_volume=("volume", "mean"),
        num_trades=("price", "count"),
        opening_timestamp=("timestamp", lambda x: x.iloc[0]),
        closing_timestamp=("timestamp", lambda x: x.iloc[-1])
    ).reset_index()

    return aggregated_trades_dataframe

@task
def enrich_data(aggregated_dataframe: object, api_key:str):
    """Adds relevant news data to the aggregated dataframe.
    
    Args:
        aggregated_dataframe (object): A dataframe containing aggregated trades data.
        api_key (string): The api key used to authenticate the api connection.
    """
    enriched_dataframe = aggregated_dataframe
    endpoint = f"https://finnhub.io/api/v1/news?category=general&token={api_key}"
    response = requests.get(url=endpoint)
    if response.status_code == 200:
        enriched_dataframe["news"] = [[] for _ in range(len(enriched_dataframe))]
        data = response.json()
        
        for news in data:
            mask = (enriched_dataframe["ticker"].apply(lambda x: x.split("-")[0]) == news["related"]) & (enriched_dataframe["opening_timestamp"] <= datetime.fromtimestamp(int(news["datetime"])/1000))
    
        enriched_dataframe.loc[mask, "news"] = enriched_dataframe.loc[mask, "news"].apply(lambda x: x + [news])

    return enriched_dataframe

@task 
def get_records(enriched_dataframe: object):
    """Gets a list of records to insert into a mysql table.
    
    Args: 
        enriched_dataframe (object): A dataframe containing aggregated trades.
    """
    tuples_list = [tuple(x) for x in enriched_dataframe.itertuples(index=False, name=None)]

    insert_string = ""

    for record in tuples_list: 
        if len(record) == 12:
            record = record + ('{}',)
        insert_string += f"{str(record)}, "
    
    return insert_string.rstrip(", ")
            
