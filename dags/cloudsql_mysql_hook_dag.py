from airflow import DAG
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# Function to fetch and print data using CloudSQLHook
def fetch_and_print_data(**kwargs):
    # Initialize the CloudSQLHook
    cloud_sql_hook = CloudSQLHook(
        #default_conn_name='gcp_mysql_connid',
        conn_id='gcp_mysql_connid',
        #sql_conn_id='gcp_mysql_connid',
        api_version='v1beta1',          # Your Cloud SQL connection ID
    )
    
    # Fetch data from Cloud SQL
    sql_query = "SELECT * FROM airbytetesting.employees;"  # Your SQL query
    conn = cloud_sql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_query)
    
    # Fetch all results
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    
    # Print the results to Airflow logs
    if rows:
        logging.info("Query Results:")
        logging.info(f"Column Names: {column_names}")
        for row in rows:
            logging.info(row)
    else:
        logging.info("No results found or query failed.")
    
    # Close the cursor and connection
    cursor.close()
    conn.close()

# Define the DAG
with DAG(
    dag_id='cloudsql_hook_print_logs',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
    tags=['example'],
) as dag:

    # Task to fetch and print data using CloudSQLHook
    print_results_task = PythonOperator(
        task_id='fetch_and_print_data',
        python_callable=fetch_and_print_data,
        provide_context=True,
    )

    # Define task dependencies (if any)
    print_results_task
