import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

FINHUB_API_KEY = os.environ.get("FINHUB_API_KEY")
KAFKA_DATABASE_USER = os.environ.get("KAFKA_DATABASE_USER")
KAFKA_DATABASE_PASSWORD = os.environ.get("KAFKA_DATABASE_PASSWORD")
AIRFLOW_DATABASE_PASSWORD = os.environ.get("AIRFLOW_DATABASE_PASSWORD")
AIRFLOW_DATABASE_USER = os.environ.get("AIRFLOW_DATABASE_USER")

DATABASE_NAME = "FINANCE_DATABASE"
STOCK_LIST = ["MSTR", "COIN", "BNB", "BTC", "ETH"]