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
from tasks.transform_data import aggregate_data, enrich_data, get_records
from tasks.sql_statements import CREATE_TABLE, INSERT_INTO_TABLE, GET_RECORDS
from tasks.analyse_data import plot_analysis
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config.settings import STOCK_LIST, DATABASE_NAME, KAFKA_BROKER, BACKUP_KAFKA_BROKER, FINHUB_API_KEY
from utils.utils import Utils

default_args = {
    "start_date":datetime(year=2025, month=3, day=14),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "tickers": STOCK_LIST,
    "raw_data":{},
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
        # Ittirates over the topics in the kafka broker.
        for topic in default_args["tickers"]:
            
            with TaskGroup(group_id=f"Extract_{topic}_Data",
                           parent_group=Extract_Data) as ExtractTopicData:
                
                # Selects how far back to read logs.
                length_of_logs_to_read = select_extract_tasks(topic=topic)

                last_10_minutes_of_logs = read_kafka_logs.override(task_id="read_logs_back_ten_minutes")(topic=topic, 
                                                                                                         broker="broker:9092",
                                                                                                         minutes = 10)
                all_logs.append(last_10_minutes_of_logs)
                
                last_hour_of_logs = read_kafka_logs.override(task_id="read_logs_back_one_hour")(topic=topic, 
                                                                                                broker="broker:9092",
                                                                                                minutes = 60)
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
        
        # Stores all logs in JSON format (a list of dictionaries)
        flattened_list = flatten_return_values(list_of_logs=all_logs)
        
        # Cleans the data.
        standardise_attributes = select_relevant_columns(raw_trades_data=flattened_list)

        cleaned_timestamp = clean_timestamp(list_of_logs=standardise_attributes)

        (
            flattened_list
            >> standardise_attributes
            >> cleaned_timestamp
        )

    with TaskGroup(group_id="Transform_Data") as Transform_Data:
        
        # Aggregates the data and adds aggregted columns.
        aggregated_trades = aggregate_data(cleaned_trade_list=cleaned_timestamp)

        # Adds data on ticker symbol news if there has been any.
        enriched_trades = enrich_data(aggregated_dataframe=aggregated_trades,
                                      api_key=FINHUB_API_KEY)


        (
            aggregated_trades
            >> enriched_trades
        )

    with TaskGroup(group_id="Load_Data") as Load_Data:

        records = get_records(enriched_dataframe=enriched_trades)

        formatted_insert_query = INSERT_INTO_TABLE.format(records)

        
        create_table = SQLExecuteQueryOperator(task_id="create_table",
                                               sql=CREATE_TABLE,
                                               autocommit=True,
                                               conn_id="mysql_finance_db",
                                               database=DATABASE_NAME)
        
        # Inserts records into a MySQL database
        load_records = SQLExecuteQueryOperator(task_id="load_records",
                                               sql=formatted_insert_query,
                                               autocommit=True,
                                               conn_id="mysql_finance_db",
                                               database=DATABASE_NAME)
        
        (
            records
            >> create_table
            >> load_records
        )

    with TaskGroup(group_id="Visualise_Data") as Visualise_Data:

        # Fetches all records from the access layer table.
        records= SQLExecuteQueryOperator(task_id="fetch_records",
                                               sql=GET_RECORDS,
                                               autocommit=True,
                                               conn_id="mysql_finance_db",
                                               database=DATABASE_NAME)
        
        graph = plot_analysis(data=records.output)
        
        (
            records
            >> graph
        )
        

    (
        Extract_Data
        >> Clean_Data
        >>Transform_Data
        >> Load_Data
        >> Visualise_Data
    )
