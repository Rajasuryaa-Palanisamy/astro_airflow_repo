import os
import time
from datetime import datetime, timedelta,date
from airflow.operators.dummy_operator import DummyOperator
from airflow import models
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
from google.cloud import bigquery
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow import DAG
from google.cloud import storage
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.bigquery.table import TimePartitioning
import pytz
import pandas as pd
import numpy as np
from airflow.utils.email import send_email
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
#from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

def bq_merge_query(table_name):
    # Construct the path to the SQL file using os.path.join
    file_path = os.path.join(os.environ['AIRFLOW_HOME'], 'include', 'merge_account_contact_relation2.sql')

    # Ensure the file path is correct
    print(f"Looking for SQL file at: {file_path}")

    # Open and read the SQL file
    with open(file_path, 'r') as file:
        sql_query = file.read()

    # Modify the query if needed using table_name
    # ...

    return sql_query

default_args = {
    'owner': 'aira',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': datetime.timedelta(minutes=5)
}


with models.DAG(
    'dag-test_getsql',
    start_date= days_ago(1),
    schedule_interval=None, 
    catchup=False,
    default_args=default_args,
) as dag:
    
    tsk_aira_bqcur_upsert_product2 = bq_merge_query()

    tsk_aira_bqcur_upsert_product2

