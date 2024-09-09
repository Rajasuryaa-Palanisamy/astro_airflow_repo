from airflow import DAG
from airflow.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
    'example_mysql_dag',
    default_args=default_args,
    description='A simple DAG to query MySQL',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    query_task = SQLExecuteQueryOperator(
        task_id='execute_mysql_query',
        sql='SELECT * FROM employees;',
        mysql_conn_id='mysql_default',
    )

    query_task
