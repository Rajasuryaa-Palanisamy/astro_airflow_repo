import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import recurly
import time
import logging
from datetime import datetime
import argparse


# Custom serialization function to convert custom objects to JSON-serializable dicts
def serialize(obj):
    if hasattr(obj, "__dict__"):
        return obj.__dict__  # Convert to dict using __dict__ for custom class instances
    elif isinstance(obj, list):
        return [serialize(item) for item in obj]  # Recursively handle lists of objects
    else:
        return obj  # Return as is if already serializable (e.g., str, int)

# Function to fetch all accounts from Recurly with pagination
def fetch_all_accounts(client,my_variable):
    accounts_data = []
    try:
        limit = 200  # Define the number of items per request
        begin_time = my_variable    #"2022-08-13 02:00:01+00:00"
        # Initialize the pager to handle pagination
        accounts = client.list_accounts(params={'limit': limit,'begin_time': begin_time}).items()
        
        # Loop through the accounts to fetch all accounts
        for account in accounts:
            # Append account details to the list

            # print("\n ",account)
            # print("\n ",type(account))

            # print("\n ",account.address)
            # print("\n ",type(account.address))

            # print(serialize(account.address))
            # print(type(serialize(account.address)))


            accounts_data.append({
                "id": account.id,
                "code": account.code,
                "email": account.email,
                "first_name": account.first_name,
                "last_name": account.last_name,
                "updated_at": account.updated_at,
                "object": account.object,
                "state":account.state,
                "hosted_login_token": account.hosted_login_token,
                "created_at": account.created_at,
                "deleted_at":account.deleted_at,
                "username" : account.username,
                "preferred_locale" : account.preferred_locale,
                "cc_emails" : account.cc_emails,
                "company" : account.company,
                "vat_number" : account.vat_number,
                "tax_exempt" : account.tax_exempt,
                "bill_to" : account.bill_to,
                "address" : str(serialize(account.address)),
                "billing_info" : str(serialize(account.billing_info)),    
            })

    except Exception as e:
        print(f"An error occurred while fetching accounts: {e}")

    return accounts_data
    

# Define the DoFn to fetch and process the data
class FetchAccountsDoFn(beam.DoFn):
    def process(self, my_variable):
        print("my_variable : ",my_variable)
        # Assume client is initialized and available globally
        accounts_data = fetch_all_accounts(client,my_variable)
        for account in accounts_data:
            # print(account)
            # print(type(account))
            yield account
            

def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('--my_variable', type=str, help='A variable passed from Airflow DAG')
    args, beam_args = parser.parse_known_args(argv)

    # Use the variable in your Beam pipeline
    my_variable = args.my_variable
    print(f"Received variable: {my_variable}")

    # Initialize the pipeline options
    options = PipelineOptions(
        runner='DataflowRunner',
        project='searce-dna-ps1-delivery',  # Update with your project ID
        job_name='recurly-to-bq-test',
        temp_location='gs://mssql-bq-acc/dataflow_temp',  # Update with your GCS bucket
        region='us-central1',
        machine_type="n1-standard-4",
        worker_region="us-central1",
        num_workers=2,  # DEFINES THE NUMBER OF WORKERS IN WORKER POOL
        max_num_workers=2,  
        service_account_email="recurly-sa@searce-dna-ps1-delivery.iam.gserviceaccount.com",
        #subnetwork="regions/us-west1/subnetworks/gfg-pvt-us-wt1subnet",
        requirements_file= "/home/rajasuryaa_palanisamy/np_digital/dynamodb/requirements.txt"

    )

    # BigQuery table schema
    schema = {
        'fields': [
            {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'updated_at', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'object', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'hosted_login_token', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'deleted_at', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'username', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'preferred_locale', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cc_emails', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'company', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'vat_number', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'tax_exempt', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'bill_to', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'billing_info', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

    # BigQuery table specification
    table_spec = 'searce-dna-ps1-delivery:recurly_raw.accounts'

    # Create a Beam pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Start' >> beam.Create([None])  # Dummy element to initiate the pipeline
            | 'FetchAccounts' >> beam.ParDo(FetchAccountsDoFn(my_variable))  # Fetch accounts data
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table_spec,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,  # Use WRITE_APPEND if you want to append
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':

    start = datetime.now()
    # Assume the Recurly client is set up correctly
    client = recurly.Client(api_key='753f4843000b44d29e4809d8d4a89cd0')  # Replace with your actual API key

    logging.getLogger().setLevel(logging.INFO)
    # Run the Apache Beam pipeline
    run()


    end = datetime.now()

    td = (end - start).total_seconds()
    print(f"The time of execution of the above program is : {td} s")
