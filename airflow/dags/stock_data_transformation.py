from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.hooks.sql import fetch_all_handler

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tasks.clean_data import initialise_tables, get_table_names, clean_raw_data
from tasks.transform_data import select_aggregation_tasks
from tasks.sql_scripts import READ_RAW_DATA, INSERT_CLEANED_DATA, AGGREGATE_DATA, UNION_DATA
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config.settings import AIRFLOW_DATABASE_PASSWORD, AIRFLOW_DATABASE_USER
from utils.utils import Utils

default_args = {
    "start_date":datetime(year=2025, month=3, day=12),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "tickers": ["BINANCE:BTCUSDT", "AMZN"],
    "database_name": "FINANCE_DATABASE",
    "database_user": "airflow_user",
    "database_password": "password",
}
default_args["table_names"] = Utils.update_topic_names(topics=default_args["tickers"])

with DAG(
    dag_id="Stock_Data_Transformation_Dag",
    description="This dag contains tasks to build a stock trade database, clean raw trade data, add the data to tables in the database, and aggregate and join  the data together.",
    default_args=default_args,
    schedule_interval=timedelta(minutes=10)
) as dag:
    
    with TaskGroup(group_id="Build_Tables") as Build_Tables:

        get_table_sufixxes = get_table_names(tickers=default_args["tickers"],
                                          )
    
        initialise_tables = initialise_tables(ticker_symbols=default_args["tickers"], 
                                          database_name=default_args["database_name"], 
                                          user=default_args["database_user"], 
                                          password=default_args["database_password"]
                                          )
        
        (
            get_table_sufixxes
            >> initialise_tables
        )
    

    
    with TaskGroup(group_id="Clean_Data") as Clean_Data:

        query_reference_trade_code_table = SQLExecuteQueryOperator(
            task_id="query_reference_trade_code_table",
            sql="SELECT * FROM trade_conditions_reference_data;",
            autocommit=True,
            conn_id="mysql_finance_db",
            database=default_args["database_name"],
            handler=Utils.cursor_results_to_dict
            )

        for stock in default_args['table_names']:

            raw_query = READ_RAW_DATA.format(f"raw_{default_args['table_names'][stock]}",
                                 f"cleaned_{default_args['table_names'][stock]}")

            query_raw_tables = SQLExecuteQueryOperator(
                task_id=f"query_raw_{default_args['table_names'][stock]}_table",
                sql=raw_query,
                autocommit=True,
                conn_id="mysql_finance_db",
                database=default_args["database_name"],
                handler=Utils.cursor_results_to_dict
                )
            
            clean_data = clean_raw_data.override(task_id=f"clean_{default_args['table_names'][stock]}_data")(
                raw_query_result=query_raw_tables.output,
                reference_codes=query_reference_trade_code_table.output
            )

            insert_cleaned_query = INSERT_CLEANED_DATA.format(
                f"cleaned_{default_args['table_names'][stock]}",
                clean_data)
            
            insert_into_clean_tables = SQLExecuteQueryOperator(
                task_id=f"insert_into_cleaned_{default_args['table_names'][stock]}_table",
                sql=insert_cleaned_query,
                autocommit=True,
                conn_id="mysql_finance_db",
                database=default_args["database_name"]
            )

            (
                query_raw_tables
                >>clean_data
                >>insert_into_clean_tables
            )
        
            
    with TaskGroup(group_id="Transform_Data") as Transform_Data:

        union_query = UNION_DATA

        for symbol in default_args["table_names"]:

            ten_minute_aggregation_query = AGGREGATE_DATA.format(
                f"aggregated_{default_args["table_names"][symbol]}",
                "10",
                f"cleaned_{default_args["table_names"][symbol]}",
                f"cleaned_{default_args["table_names"][symbol]}",
                f"cleaned_{default_args["table_names"][symbol]}",
                "10"
            )

            aggregate_data_over_ten_minutes = SQLExecuteQueryOperator(
                task_id=f"aggregate_{default_args["table_names"][symbol]}_data_over_ten_minutes",
                sql=ten_minute_aggregation_query,
                autocommit=True,
                conn_id="mysql_finance_db",
                database=default_args["database_name"]
            )

            one_hour_aggregation_query = AGGREGATE_DATA.format(
                f"aggregated_{default_args["table_names"][symbol]}",
                "60",
                f"cleaned_{default_args["table_names"][symbol]}",
                f"cleaned_{default_args["table_names"][symbol]}",
                f"cleaned_{default_args["table_names"][symbol]}",
                "60"
            )

            aggregate_data_over_one_hour = SQLExecuteQueryOperator(
                task_id=f"aggregate_{default_args["table_names"][symbol]}_data_over_one_hour",
                sql=one_hour_aggregation_query,
                autocommit=True,
                conn_id="mysql_finance_db",
                database=default_args["database_name"]
            )

            one_day_aggregation_query = AGGREGATE_DATA.format(
                f"aggregated_{default_args["table_names"][symbol]}",
                "1440",
                f"cleaned_{default_args["table_names"][symbol]}",
                f"cleaned_{default_args["table_names"][symbol]}",
                f"cleaned_{default_args["table_names"][symbol]}",
                "1440"
            )

            aggregate_data_over_one_day = SQLExecuteQueryOperator(
                task_id=f"aggregate_{default_args["table_names"][symbol]}_data_over_one_day",
                sql=one_day_aggregation_query,
                autocommit=True,
                conn_id="mysql_finance_db",
                database=default_args["database_name"]
            )

            branch_aggregation_tasks = select_aggregation_tasks.override(
                task_id=f"select_{default_args["table_names"][symbol]}_aggregation_tasks"
                )(table_name=default_args["table_names"][symbol]
                  )
            
            union_query = f"{union_query} SELECT * FROM aggregated_{default_args["table_names"][symbol]} UNION"

            (
                branch_aggregation_tasks
                >> [aggregate_data_over_one_hour, aggregate_data_over_one_day, aggregate_data_over_ten_minutes]
            )

        union_query = union_query[0:-6] + ";"

        union_data = SQLExecuteQueryOperator(
            task_id=f"union_data",
            sql=union_query,
            autocommit=True,
            conn_id="mysql_finance_db",
            database=default_args["database_name"]
        )


        (
            branch_aggregation_tasks
            >> aggregate_data_over_ten_minutes
            >> union_data
        )





    
    (
        Build_Tables
        >> Clean_Data
        >> Transform_Data
    )
    
    
    
    
    
    
    
    
    

    
    