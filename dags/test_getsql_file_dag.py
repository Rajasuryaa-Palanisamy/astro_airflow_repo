from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Function to read the SQL file
def read_sql_file():
    # Construct the path to the SQL file using os.path.join
    file_path = os.path.join(os.environ['AIRFLOW_HOME'], 'include', 'merge_account_contact_relation2.sql')

    # Ensure the file path is correct
    print(f"Looking for SQL file at: {file_path}")

    # Read the SQL file from the include directory
    try:
        with open(file_path, 'r') as file:
            sql_query = file.read()
            print(sql_query)  # Optional: Print the SQL query to the Airflow logs
    except FileNotFoundError:
        print(f"File not found at path: {file_path}")

# Define the DAG
with DAG(
    dag_id='example_read_sql_dag',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to read SQL from file
    read_sql_task = PythonOperator(
        task_id='read_sql_file_task',
        python_callable=read_sql_file,
    )
