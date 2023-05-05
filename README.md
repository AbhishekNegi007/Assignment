# Docker - python - Airflow - postgres
ETL Pipeline for a Sales Dataset
This is an ETL pipeline that extracts sales data from a CSV file, transforms it, and loads it into a PostgreSQL database.

Requirements
Python 3
pandas library
sqlalchemy library
PostgreSQL database

Configuration
Before running the ETL pipeline, you need to configure the PostgreSQL database connection details in the docker-compose file.
Replace localhost with the hostname of your PostgreSQL database server,

Airflow Dag
Open a web browser and navigate to the Airflow web interface: http://localhost:8080
Turn on the etl_pipeline DAG.
The DAG will run daily and execute the ETL pipeline
