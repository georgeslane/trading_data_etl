-- This file creates necessary mysql users when the containerised mysql database runs.
CREATE DATABASE IF NOT EXISTS airflow;

CREATE USER 'kafka_consumer'@'%' IDENTIFIED BY 'password';
CREATE USER 'airflow_user'@'%' IDENTIFIED BY 'password';
CREATE USER 'airflow'@'%' IDENTIFIED BY 'airflow';


GRANT ALL PRIVILEGES ON FINANCE_DATABASE.* TO 'kafka_consumer'@'%';
GRANT ALL PRIVILEGES ON FINANCE_DATABASE.* TO 'airflow'@'%';
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%';


FLUSH PRIVILEGES;