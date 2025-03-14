-- This file creates necessary mysql users when the containerised mysql database runs.
CREATE DATABASE IF NOT EXISTS airflow;

CREATE USER 'kafka_user'@'%' IDENTIFIED BY 'password';
CREATE USER 'airflow_user'@'%' IDENTIFIED BY 'password';
CREATE USER 'airflow'@'%' IDENTIFIED BY 'airflow';
CREATE USER 'analytics_user'@'%' IDENTIFIED BY 'password';


GRANT ALL PRIVILEGES ON FINANCE_DATABASE.* TO 'kafka_user'@'%';
GRANT ALL PRIVILEGES ON FINANCE_DATABASE.* TO 'airflow_user'@'%';
GRANT ALL PRIVILEGES ON FINANCE_DATABASE.* TO 'analytics_user'@'%';
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%';


FLUSH PRIVILEGES;