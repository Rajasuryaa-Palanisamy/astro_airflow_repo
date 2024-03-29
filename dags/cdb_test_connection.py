"""
Implements a retention policy by dropping expired partitions

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913

Prerequisites
-------------
In CrateDB, tables for storing retention policies need to be created once manually.
See the file setup/data_retention_schema.sql in this repository.
"""
from pathlib import Path
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator


@task_group
def get_policies():
   print("entered sql query op function")
   SQLExecuteQueryOperator(
            task_id="test_cdb_connection",
            conn_id="cratedb_connection",
            sql="""
                    INSERT INTO retention_policies (table_schema, table_name, partition_column, retention_period, strategy) VALUES ('doc', 'test_insert', 'ts_day', 1, 'delete');
                """)
@dag(
    start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def data_retention_delete_test():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    tg1 = get_policies()



data_retention_delete_test()
