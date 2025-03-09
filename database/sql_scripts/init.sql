-- This file creates necessary mysql users when the containerised mysql database runs.
CREATE USER 'kafka_consumer'@'%' IDENTIFIED BY 'password';
CREATE USER 'airflow_user'@'%' IDENTIFIED BY 'password';


GRANT ALL PRIVILEGES ON FINANCE_DATABASE.* TO 'kafka_consumer'@'%';
GRANT ALL PRIVILEGES ON FINANCE_DATABASE.* TO 'airflow'@'%';


FLUSH PRIVILEGES;