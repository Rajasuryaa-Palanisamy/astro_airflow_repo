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
DATASET_RAW = "salesforce_raw"
# DATASET_RPT = Variable.get("GCP_BQ_DATASET_RPT")
DATASET_RPT = "salesforce_curated"

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
        max_date =  Variable.get('v_src_salesforce2bqraw_{}_max_{}'.format(bigquery_table_name, bq_table_cdc_column_name )) 
   
    print(max_date)
    Variable.set('v_src_salesforce2bqraw_{}_max_{}'.format(bigquery_table_name, bq_table_cdc_column_name ), max_date) 
    '''Variable.set('v_src_edw2gcsraw_account_dim_max_EffectiveDt','1999-01-01') '''

def dataflow_job(source_table_name, src_table_cdc_column_name):
    
    max_date = Variable.get('v_src_salesforce2bqraw_{}_max_{}'.format(source_table_name,src_table_cdc_column_name))

    run_df_job = BashOperator(
    task_id='aira_dfj_salesforce2bqraw_{}'.format(source_table_name),
    #$AIRFLOW_HOME/include/my_bash_script.sh
    bash_command='python3 $AIRFLOW_HOME/include/salesforce_to_bq_{}_df.py "{}" \   --requirements_file="$AIRFLOW_HOME/include/requirements.txt"'.format(source_table_name,max_date) ,
    dag=dag
    )
    return run_df_job


def bq_merge_query(table_name):
    # client = storage.Client()
    # bucket = client.get_bucket(BUCKET)
    file_path = '$AIRFLOW_HOME/include/merge_' + table_name + '.sql'
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
    'dag-salesforce-2bqraw-bash',
    start_date= days_ago(1),
    schedule_interval=None, 
    catchup=False,
    default_args=default_args,
) as dag:

    #Operators
    
    
    tsk_aira_src_salesforce2bqraw_dataload_start = DummyOperator(task_id="aira_src_salesforce2bqraw_dataload_start",trigger_rule="all_success")

    tsk_aira_dfj_src_salesforce_accountcontactrelation2bqraw =  dataflow_job("account_contact_relation","SystemModstamp")
    tsk_aira_dfj_src_salesforce_account2bqraw =  dataflow_job("account","SystemModstamp")
    # tsk_aira_dfj_src_salesforce_accountcontactrole2bqraw =  dataflow_job("account_contact_role","SystemModstamp")
    tsk_aira_dfj_src_salesforce_user2bqraw =  dataflow_job("user","SystemModstamp")
    tsk_aira_dfj_src_salesforce_opportunity2bqraw =  dataflow_job("opportunity","SystemModstamp")
    # tsk_aira_dfj_src_salesforce_usage_c2bqraw =  dataflow_job("usage_c","SystemModstamp")
    tsk_aira_dfj_src_salesforce_contact2bqraw =  dataflow_job("contact","SystemModstamp")
    tsk_aira_dfj_src_salesforce_product22bqraw =  dataflow_job("product2","SystemModstamp")

    
    tsk_aira_bqcur_upsert_account_contact_relation = updated_bq_cur_table("merge_query_job_account_contact_relation","account_contact_relation2")
    tsk_aira_bqcur_upsert_account = updated_bq_cur_table("merge_query_job_account","account")
    # tsk_aira_bqcur_upsert_account_contact_role = updated_bq_cur_table("merge_query_job_account_contact_role","account_contact_role")
    tsk_aira_bqcur_upsert_user = updated_bq_cur_table("merge_query_job_user","user")
    tsk_aira_bqcur_upsert_opportunity = updated_bq_cur_table("merge_query_job_opportunity","opportunity")
    # tsk_aira_bqcur_upsert_usage_c = updated_bq_cur_table("merge_query_job_usage_c","usage_c")
    tsk_aira_bqcur_upsert_contact = updated_bq_cur_table("merge_query_job_contact","contact")
    tsk_aira_bqcur_upsert_product2 = updated_bq_cur_table("merge_query_job_product2","product2")

    tsk_aira_bq_set_max_date_account_contact_relation = PythonOperator(task_id="set_max_date_account_contact_relation",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'account_contact_relation','bq_table_cdc_column_name':"SystemModstamp",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_account = PythonOperator(task_id="set_max_date_account",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'account','bq_table_cdc_column_name':"SystemModstamp",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    # tsk_aira_bq_set_max_date_account_contact_role = PythonOperator(task_id="set_max_date_account_contact_role",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'account_contact_role','bq_table_cdc_column_name':"SystemModstamp",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_user = PythonOperator(task_id="set_max_date_user",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'user','bq_table_cdc_column_name':"SystemModstamp",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_opportunity = PythonOperator(task_id="set_max_date_opportunity",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'opportunity','bq_table_cdc_column_name':"SystemModstamp",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    # tsk_aira_bq_set_max_date_usage_c = PythonOperator(task_id="set_max_date_usage_c",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'usage_c','bq_table_cdc_column_name':"SystemModstamp",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_contact = PythonOperator(task_id="set_max_date_contact",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'contact','bq_table_cdc_column_name':"SystemModstamp",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)
    tsk_aira_bq_set_max_date_product2 = PythonOperator(task_id="set_max_date_product2",python_callable=set_max_date,op_kwargs={'project_id':PROJECT_ID,'dataset':DATASET_RAW,'bigquery_table_name':'product2','bq_table_cdc_column_name':"SystemModstamp",'dag':dag},provide_context=True,trigger_rule = 'all_done',dag=dag)

    tsk_aira_src_salesforce2bqraw_dataload_end = DummyOperator(task_id="aira_src_salesforce2bqraw_dataload_end",trigger_rule="all_success")

    tsk_aira_src_salesforce2bqraw_dataload_start >> tsk_aira_dfj_src_salesforce_accountcontactrelation2bqraw >> tsk_aira_bqcur_upsert_account_contact_relation >> tsk_aira_bq_set_max_date_account_contact_relation >> tsk_aira_src_salesforce2bqraw_dataload_end
    tsk_aira_src_salesforce2bqraw_dataload_start >> tsk_aira_dfj_src_salesforce_account2bqraw >> tsk_aira_bqcur_upsert_account >> tsk_aira_bq_set_max_date_account >> tsk_aira_src_salesforce2bqraw_dataload_end
    # tsk_aira_src_salesforce2bqraw_dataload_start >> tsk_aira_dfj_src_salesforce_accountcontactrole2bqraw >> tsk_aira_bqcur_upsert_account_contact_role >> tsk_aira_bq_set_max_date_account_contact_role >> tsk_aira_src_salesforce2bqraw_dataload_end
    tsk_aira_src_salesforce2bqraw_dataload_start >> tsk_aira_dfj_src_salesforce_user2bqraw >> tsk_aira_bqcur_upsert_user >> tsk_aira_bq_set_max_date_user >> tsk_aira_src_salesforce2bqraw_dataload_end
    tsk_aira_src_salesforce2bqraw_dataload_start >> tsk_aira_dfj_src_salesforce_opportunity2bqraw >> tsk_aira_bqcur_upsert_opportunity >> tsk_aira_bq_set_max_date_opportunity >> tsk_aira_src_salesforce2bqraw_dataload_end
    # tsk_aira_src_salesforce2bqraw_dataload_start >> tsk_aira_dfj_src_salesforce_usage_c2bqraw >> tsk_aira_bqcur_upsert_usage_c >> tsk_aira_bq_set_max_date_usage_c >> tsk_aira_src_salesforce2bqraw_dataload_end
    tsk_aira_src_salesforce2bqraw_dataload_start >> tsk_aira_dfj_src_salesforce_contact2bqraw >> tsk_aira_bqcur_upsert_contact >> tsk_aira_bq_set_max_date_contact >> tsk_aira_src_salesforce2bqraw_dataload_end
    tsk_aira_src_salesforce2bqraw_dataload_start >> tsk_aira_dfj_src_salesforce_product22bqraw >> tsk_aira_bqcur_upsert_product2 >> tsk_aira_bq_set_max_date_product2 >> tsk_aira_src_salesforce2bqraw_dataload_end
