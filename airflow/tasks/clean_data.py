from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

@task
def select_relevant_columns(raw_trades_data: list):
    """Returns only desired attributes from the raw_trades_data.
    
    Args:
        raw_trades_data (list): A list containing trades.
    """
    updated_list = []
    for trade in raw_trades_data:
        updated_trade = {}
        if trade["source"] == "Finnhub":
            updated_trade["ticker"] = trade["s"]
            updated_trade["price"] = float(trade["p"])
            updated_trade["timestamp"] = trade["t"]
            updated_trade["volume"] = float(trade["v"])
        elif trade["source"] == "Coinbase":
            updated_trade["ticker"] = trade["product_id"]
            updated_trade["price"] = float(trade["price"])
            updated_trade["timestamp"] = trade["time"]
            updated_trade["volume"] = float(trade["last_size"])
        updated_trade["source"] = trade["source"]
        updated_trade["aggregation_period"] = trade["aggregation_period"]
        updated_list.append(updated_trade)
    return updated_list

@task(trigger_rule=TriggerRule.ALL_DONE)
def flatten_return_values(list_of_logs: list):
    """Flattens a list of lists of logs into a list of logs.
    
    Args:
        list_of_logs (string): A nested list containing lists of kafka logs.
    """
    updated_list = []
    unpacked_list = []
    print(len(list_of_logs))
    for item in list_of_logs:
        if item is not None and item != []:
            updated_list.append(item)
    for item in updated_list:
        for trade in item:
            unpacked_list.append(trade)
    return unpacked_list
            
@task
def clean_timestamp(list_of_logs: list):
    updated_list = []
    for trade in list_of_logs:
        updated_trade = trade
        if trade["source"] == "Finnhub":
            updated_trade["timestamp"] = datetime.fromtimestamp(int(trade["timestamp"])/1000)
        elif trade["source"] == "Coinbase":
            updated_trade["timestamp"] = datetime.strptime(str(trade["timestamp"]).split(".")[0], "%Y-%m-%dT%H:%M:%S")
        updated_list.append(updated_trade)
    return updated_list

                
            

