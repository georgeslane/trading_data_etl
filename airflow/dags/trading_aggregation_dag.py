from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.operators.empty import EmptyOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tasks.read_kafka import read_kafka_logs, select_extract_tasks
from tasks.clean_data import select_relevant_columns, flatten_return_values, clean_timestamp
from tasks.transform_data import aggregate_data, enrich_data
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config.settings import STOCK_LIST, DATABASE_NAME, KAFKA_BROKER, BACKUP_KAFKA_BROKER
from utils.utils import Utils

default_args = {
    "start_date":datetime(year=2025, month=3, day=14),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "tickers": STOCK_LIST,
    "raw_data":{},
    "database_name": DATABASE_NAME,
    "database_user": "airflow_user",
    "database_password": "password",
}

with DAG(
    dag_id="Stock_Trades_Aggregation_Dag",
    description="""This dag contains tasks to extract raw trading data from apache kafka logs, 
    clean the data, enrich and aggreate the data and add it to a database.""",
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    
    with TaskGroup(group_id="Extract_Data") as Extract_Data:

        all_logs = []
        
        for topic in default_args["tickers"]:
            
            with TaskGroup(group_id=f"Extract_{topic}_Data",
                           parent_group=Extract_Data) as ExtractTopicData:
                
                length_of_logs_to_read = select_extract_tasks(topic=topic)

                last_10_minutes_of_logs = read_kafka_logs.override(task_id="read_logs_back_ten_minutes")(topic=topic, 
                                                                                                         broker="broker:9092",
                                                                                                         minutes = 400)
                all_logs.append(last_10_minutes_of_logs)
                
                last_hour_of_logs = read_kafka_logs.override(task_id="read_logs_back_one_hour")(topic=topic, 
                                                                                                broker="broker:9092",
                                                                                                minutes = 1440)
                all_logs.append(last_hour_of_logs)
                
                last_day_of_logs = read_kafka_logs.override(task_id="read_logs_back_one_day")(topic=topic, 
                                                                                                broker="broker:9092",
                                                                                                minutes = 1440)
                all_logs.append(last_day_of_logs)

                
                (
                    length_of_logs_to_read
                    >> [last_10_minutes_of_logs, last_hour_of_logs, last_day_of_logs]
                )

            

    with TaskGroup(group_id="Clean_Data") as Clean_Data:

        flattened_list = flatten_return_values(list_of_logs=all_logs)
        
        standardise_attributes = select_relevant_columns(raw_trades_data=flattened_list)

        cleaned_timestamp = clean_timestamp(list_of_logs=standardise_attributes)

        (
            flattened_list
            >> standardise_attributes
            >> cleaned_timestamp
        )

    with TaskGroup(group_id="Transform_Data") as Transform_Data:
        
        aggregated_trades = aggregate_data(cleaned_trade_list=cleaned_timestamp)

        enriched_trades = enrich_data(aggregated_dataframe=aggregated_trades,
                                      api_key="cv0pib9r01qo8ssi6d00cv0pib9r01qo8ssi6d0g")
        
        # add_to_table = 

        (
            aggregated_trades
            >> enriched_trades
        )

    with TaskGroup(group_id="Visualise_Data") as Visualise_Data:
        pass

        
                




    (
        Extract_Data
        >> Clean_Data
        >>Transform_Data
        >> Visualise_Data
    )
