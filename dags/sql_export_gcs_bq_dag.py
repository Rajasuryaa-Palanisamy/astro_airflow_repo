from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator
from airflow import models
import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

export_body = {
  "exportContext": {
      "databases": ["airbytetesting"],
    "kind": "sql#exportContext",
    "fileType": "csv",
    "uri": "gs://mssql-bq-acc/dag_export.csv",
    "csvExportOptions": {
      "selectQuery": "select * from employees;"
    }
  }
}

def bq_load(dag,task_id,destination_project_dataset_table):
    bqload = GoogleCloudStorageToBigQueryOperator(
        task_id = task_id,
        bucket="mssql-bq-acc",
        destination_project_dataset_table = destination_project_dataset_table, 
        create_disposition='CREATE_IF_NEEDED',
        #schema_object=schema_object,
        source_format='csv',
        field_delimiter=',' ,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        autodetect=False,
        source_objects=['dag_export.csv'],
        write_disposition='WRITE_APPEND',
        skip_leading_rows=0,
        dag=dag)   
    return bqload

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
    
    gcs_to_bq = bq_load(dag,"gcsraw2bqraw",'searce-dna-ps1-delivery.aira_mysql_raw.employees')

    sql_export_task >> gcs_to_bq