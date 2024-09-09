from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
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

with DAG(
    'mysql_dag',
    default_args=default_args,
    description='A simple DAG to query MySQL',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    query_task = SQLExecuteQueryOperator(
        task_id='execute_mysql_query',
        sql='SELECT * FROM employees;',
        conn_id='mysql_default',
        return_last=True,
        show_return_value_in_logs=True,
        do_xcom_push=True,
    )

    # Task to print query results to Airflow logs
    print_results = PythonOperator(
        task_id='print_query_results',
        python_callable=print_query_results,
        provide_context=True,
    )

    query_task >> print_results
