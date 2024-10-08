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

now = datetime.now()


# PROJECT_ID = Variable.get("GCP_PROJECT_ID")
PROJECT_ID = "searce-dna-ps1-delivery"
# REGION = Variable.get("GCP_REGION")

# DATASET_RAW = Variable.get("GCP_BQ_DATASET_RAW")
DATASET_RAW = "recurly_raw"
# DATASET_RPT = Variable.get("GCP_BQ_DATASET_RPT")
DATASET_RPT = "recurly_cur"

def set_max_date(dag,project_id,dataset,bigquery_table_name, bq_table_cdc_column_name):
    client = bigquery.Client(project= project_id)
    table_ref = client.dataset(dataset).table(bigquery_table_name)
    query = """
        SELECT max(`{}`)
        FROM `{}.{}.raw_{}`
    """.format(bq_table_cdc_column_name,project_id,dataset,bigquery_table_name)
    query_job = client.query(query)
    results = query_job.to_dataframe()
    max_date = results.iloc[0, 0]
    max_date = str(max_date)
    print(max_date)
    if(max_date == "" or max_date == "None" or max_date=="NaT"): 
        max_date =  Variable.get('v_src_recurly2bqraw_{}_max_{}'.format(bigquery_table_name, bq_table_cdc_column_name )) 
   
    print(max_date)
    Variable.set('v_src_recurly2bqraw_{}_max_{}'.format(bigquery_table_name, bq_table_cdc_column_name ), max_date) 
    '''Variable.set('v_src_edw2gcsraw_account_dim_max_EffectiveDt','1999-01-01') '''

def dataflow_job(source_table_name, src_table_cdc_column_name):
    
    MAX_DATE = Variable.get('v_src_recurly2bqraw_{}_max_{}'.format(source_table_name,src_table_cdc_column_name))

    run_df_job = BeamRunPythonPipelineOperator(
        task_id='run-beam-python-pipeline',
        runner=BeamRunnerType.DataflowRunner,
        #py_file='/home/airflow/gcs/dags/aira_accounts_bq_raw_df.py',  # Path to your Apache Beam Python script
        py_file ='gs://mssql-bq-acc/recurly_accounts_to_bq_df.py',
        pipeline_options={
            'my_variable': MAX_DATE,
            'job_name': 'recurly-to-bq-test',  # Pass the variable as a pipeline option
        },
        py_options=[],
        py_interpreter='python3',
        py_requirements=['apache-beam[gcp]','google-cloud-storage','recurly','pandas'],
        py_system_site_packages=False,
    )
    return run_df_job

def updated_bq_cur_table(task_id,table_name):
    # BigQueryInsertJobOperator to run the MERGE query

    # SQL for the MERGE query
    MERGE_QUERY = """
MERGE target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
    target.column1 = source.column1,
    target.column2 = source.column2,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (id, column1, column2, updated_at)
  VALUES (source.id, source.column1, source.column2, source.updated_at);

    """

    merge_query_job = BigQueryInsertJobOperator(
        task_id='merge_query_job',
        configuration={
            "query": {
                "query": MERGE_QUERY,
                "useLegacySql": False,  # Use Standard SQL syntax
            }
        },
        dag=dag,
)
    return merge_query_job

default_args = {
    'owner': 'aira',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': datetime.timedelta(minutes=5)
}

with models.DAG(
    'dag-recurly-accounts2bqraw',
    start_date= days_ago(1),
    schedule_interval=None, 
    catchup=False,
    default_args=default_args,
) as dag:

    #Operators
    
    
    tsk_aira_src_recurly2bqraw_dataload_start = DummyOperator(task_id="aira_src_recurly2bqraw_dataload_start",trigger_rule="all_success")

    tsk_aira_dfj_src_recurly2bqraw =  dataflow_job("accounts","updated_at")
    
    tsk_aira_bqcur_upsert_table = updated_bq_cur_table("merge_query_job","accounts")

    tsk_aira_bq_set_max_date_account = PythonOperator(task_id="set_max_date_account",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'accounts','bq_table_cdc_column_name':"updated_at",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)

    tsk_aira_src_recurly2bqraw_dataload_end = DummyOperator(task_id="aira_src_recurly2bqraw_dataload_end",trigger_rule="all_success")

    tsk_aira_src_recurly2bqraw_dataload_start >> tsk_aira_dfj_src_recurly2bqraw >> tsk_aira_bqcur_upsert_table >> tsk_aira_bq_set_max_date_account >> tsk_aira_src_recurly2bqraw_dataload_end