from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs import AzureBlobStorageToGCSOperator
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from google.cloud import storage
import os
import io


# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    "container_name": "test-container-1", 
    "blob_name": "teams error image",
}

# Define the DAG
dag = DAG(
    'copy_blobs_azure_to_gcs',
    default_args=default_args,
    description='A simple DAG to copy blobs from Azure Storage to GCS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


start = DummyOperator(task_id="start")

transfer_files_to_gcs = AzureBlobStorageToGCSOperator(
    task_id="transfer_files_to_gcs",
    # AZURE arg
    file_path='teams_error_image_new.png',
    # GCP args
    bucket_name='mssql-bq-acc',
    object_name='teams_error_image_new.png',
    filename='gs://mssql-bq-acc/zip_us_poc',
    gzip=False,
    delegate_to=None,
    impersonation_chain=None,
)

end = DummyOperator(task_id="end")

start >> transfer_files_to_gcs >> end

