from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceOperator,
    CloudSQLExecuteQueryOperator,
)


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# Define the DAG
with DAG(
    dag_id='cloudsql_mysql_execute_query',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
    tags=['example'],
) as dag:

    # Task to execute a SQL query using CloudSQLExecuteQueryOperator
    execute_query = CloudSQLExecuteQueryOperator(
        task_id='execute_sql_query',
        gcp_cloudsql_conn_id='gcp_mysql_connid',  # Connection ID for Google Cloud
        sql=[
            "SELECT * FROM airbytetesting.employees;"  # Example SQL query
        ]
    )

    # If needed, you can add more tasks or further data processing steps here.

    execute_query
