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
DATASET_RPT = "recurly_curated"

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
    
    max_date = Variable.get('v_src_recurly2bqraw_{}_max_{}'.format(source_table_name,src_table_cdc_column_name))

    run_df_job = BashOperator(
    task_id='aira_dfj_recurly2bqraw_accounts',
    #$AIRFLOW_HOME/include/my_bash_script.sh
    bash_command='python3 $AIRFLOW_HOME/include/recurly_to_bq_accounts_df.py "{}"'.format(max_date) ,
    dag=dag
    )
    return run_df_job

def updated_bq_cur_table(task_id,table_name):
    # BigQueryInsertJobOperator to run the MERGE query

    # SQL for the MERGE query
    MERGE_QUERY = """
MERGE `searce-dna-ps1-delivery.recurly_curated.accounts` AS target
USING `searce-dna-ps1-delivery.recurly_raw.accounts_raw` AS source
ON target.id = source.id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
target.id = source.id,
target.code = source.code,
target.email = source.email,
target.first_name = source.first_name,
target.last_name = source.last_name,
target.updated_at = source.updated_at,
target.object = source.object,
target.state = source.state,
target.hosted_login_token = source.hosted_login_token,
target.created_at = source.created_at,
target.deleted_at = source.deleted_at,
target.username = source.username,
target.preferred_locale = source.preferred_locale,
target.cc_emails = source.cc_emails,
target.company = source.company,
target.vat_number = source.vat_number,
target.tax_exempt = source.tax_exempt,
target.bill_to = source.bill_to,
target.address = source.address,
target.billing_info = source.billing_info
WHEN NOT MATCHED THEN
  INSERT ROW
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
    'dag-recurly-accounts2bqraw-bash',
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