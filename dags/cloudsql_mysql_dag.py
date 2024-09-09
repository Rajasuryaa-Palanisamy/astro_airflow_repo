from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceOperator,
    CloudSQLExecuteQueryOperator,
)
from airflow.operators.python import PythonOperator
import logging

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# Function to fetch and print data from XCom
def print_query_results(**kwargs):
    # Fetch the query results from XCom
    ti = kwargs['ti']
    query_results = ti.xcom_pull(task_ids='execute_sql_query')
    
    # Print the results to Airflow logs
    if query_results:
        logging.info("Query Results:")
        for row in query_results:
            logging.info(row)
    else:
        logging.info("No results found or query failed.")

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
        ],
        do_xcom_push=True,
    )

    # Task to print query results to Airflow logs
    print_results = PythonOperator(
        task_id='print_query_results',
        python_callable=print_query_results,
        provide_context=True,
    )

    # Define task dependencies
    execute_query >> print_results

    # If needed, you can add more tasks or further data processing steps here.

    execute_query
