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
    #print(max_date)
    # Convert the string to a datetime object
    dt = datetime.fromisoformat(max_date)
    # Convert the datetime object to the desired format
    max_date = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
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
    bash_command='python3 $AIRFLOW_HOME/include/recurly_to_bq_{}_df.py "{}"'.format(source_table_name,max_date) ,
    dag=dag
    )
    return run_df_job

def bq_merge_query(table_name):
    # client = storage.Client()
    # bucket = client.get_bucket(BUCKET)
    file_path = '/usr/local/airflow/include/merge_' + table_name + '.sql'
    with open(file_path, 'r') as file:
        sql_query = file.read()
    # blob = bucket.blob(file_path)
    # query = blob.download_as_text()

    return sql_query

def updated_bq_cur_table(task_id,table_name):
    # BigQueryInsertJobOperator to run the MERGE query

    # SQL for the MERGE query
    MERGE_QUERY = bq_merge_query(table_name)
    
    merge_query_job = BigQueryInsertJobOperator(
        task_id=task_id,
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
    'dag-recurly-2bqraw-bash',
    start_date= days_ago(1),
    schedule_interval=None, 
    catchup=False,
    default_args=default_args,
) as dag:

    #Operators
    
    
    tsk_aira_src_recurly2bqraw_dataload_start = DummyOperator(task_id="aira_src_recurly2bqraw_dataload_start",trigger_rule="all_success")

    tsk_aira_dfj_src_recurly_accounts2bqraw =  dataflow_job("accounts","updated_at")
    tsk_aira_dfj_src_recurly_coupons2bqraw =  dataflow_job("coupons","updated_at")
    tsk_aira_dfj_src_recurly_invoices2bqraw =  dataflow_job("invoices","updated_at")
    tsk_aira_dfj_src_recurly_plans_add_ons2bqraw =  dataflow_job("plans_add_ons","updated_at")
    tsk_aira_dfj_src_recurly_plans2bqraw =  dataflow_job("plans","updated_at")
    tsk_aira_dfj_src_recurly_subscriptions2bqraw =  dataflow_job("subscriptions","updated_at")
    
    tsk_aira_bqcur_upsert_accounts = updated_bq_cur_table("merge_query_job_accounts","accounts")
    tsk_aira_bqcur_upsert_coupons = updated_bq_cur_table("merge_query_job_coupons","coupons")
    tsk_aira_bqcur_upsert_invoices = updated_bq_cur_table("merge_query_job_invoices","invoices")
    tsk_aira_bqcur_upsert_plans_add_ons = updated_bq_cur_table("merge_query_job_plans_add_ons","plans_add_ons")
    tsk_aira_bqcur_upsert_plans = updated_bq_cur_table("merge_query_job_plans","plans")
    tsk_aira_bqcur_upsert_subscriptions = updated_bq_cur_table("merge_query_job_subscriptions","subscriptions")

    tsk_aira_bq_set_max_date_accounts = PythonOperator(task_id="set_max_date_accounts",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'accounts','bq_table_cdc_column_name':"updated_at",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_coupons = PythonOperator(task_id="set_max_date_coupons",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'coupons','bq_table_cdc_column_name':"updated_at",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_invoices = PythonOperator(task_id="set_max_date_invoices",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'invoices','bq_table_cdc_column_name':"updated_at",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_plans_add_ons = PythonOperator(task_id="set_max_date_plans_add_ons",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'plans_add_ons','bq_table_cdc_column_name':"updated_at",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_plans = PythonOperator(task_id="set_max_date_plans",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'plans','bq_table_cdc_column_name':"updated_at",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_subscriptions = PythonOperator(task_id="set_max_date_subscriptions",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'subscriptions','bq_table_cdc_column_name':"updated_at",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)

    tsk_aira_src_recurly2bqraw_dataload_end = DummyOperator(task_id="aira_src_recurly2bqraw_dataload_end",trigger_rule="all_success")

    tsk_aira_src_recurly2bqraw_dataload_start >> tsk_aira_dfj_src_recurly_accounts2bqraw >> tsk_aira_bqcur_upsert_accounts >> tsk_aira_bq_set_max_date_accounts >> tsk_aira_src_recurly2bqraw_dataload_end
    tsk_aira_src_recurly2bqraw_dataload_start >> tsk_aira_dfj_src_recurly_coupons2bqraw >> tsk_aira_bqcur_upsert_coupons >> tsk_aira_bq_set_max_date_coupons >> tsk_aira_src_recurly2bqraw_dataload_end
    tsk_aira_src_recurly2bqraw_dataload_start >> tsk_aira_dfj_src_recurly_invoices2bqraw >> tsk_aira_bqcur_upsert_invoices >> tsk_aira_bq_set_max_date_invoices >> tsk_aira_src_recurly2bqraw_dataload_end
    tsk_aira_src_recurly2bqraw_dataload_start >> tsk_aira_dfj_src_recurly_plans_add_ons2bqraw >> tsk_aira_bqcur_upsert_plans_add_ons >> tsk_aira_bq_set_max_date_plans_add_ons >> tsk_aira_src_recurly2bqraw_dataload_end
    tsk_aira_src_recurly2bqraw_dataload_start >> tsk_aira_dfj_src_recurly_plans2bqraw >> tsk_aira_bqcur_upsert_plans >> tsk_aira_bq_set_max_date_plans >> tsk_aira_src_recurly2bqraw_dataload_end
    tsk_aira_src_recurly2bqraw_dataload_start >> tsk_aira_dfj_src_recurly_subscriptions2bqraw >> tsk_aira_bqcur_upsert_subscriptions >> tsk_aira_bq_set_max_date_subscriptions >> tsk_aira_src_recurly2bqraw_dataload_end