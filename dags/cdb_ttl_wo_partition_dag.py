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
                    DELETE from keywords_random_ip_ttl_test WHERE TIMESTAMP < 1699276275560;
                """)
@dag(
    start_date=pendulum.datetime(2022, 11, 19, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def ttl_delete_test():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    tg1 = get_policies()



ttl_delete_test()
