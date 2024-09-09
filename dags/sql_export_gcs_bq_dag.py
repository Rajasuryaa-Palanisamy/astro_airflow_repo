from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator
from airflow import models
import datetime
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

export_body = {
  "exportContext": {
    "kind": "sql#exportContext",
    "fileType": "csv",
    "uri": "gs://mssql-bq-acc/dag_export.csv",
    "csvExportOptions": {
      "selectQuery": "select * from employees;"
    }
  }
}

with DAG(
    'sql_export_gcs_bq',
    default_args=default_args,
    description='A simple DAG to export MySQL',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    sql_export_task = CloudSqlInstanceExportOperator(body=export_body,
                                                     project_id="searce-dna-ps1-delivery",
                                                     instance='airbytepoc',
                                                     task_id='sql_export_task')
    sql_export_task