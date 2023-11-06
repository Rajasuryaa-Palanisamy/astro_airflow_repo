
from pathlib import Path
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task


@dag(
    start_date=pendulum.datetime(2023, 11, 19, tz="UTC"),
    schedule="@daily",
    catchup=False,
)

@task
def ttl_delete_test():
    SQLExecuteQueryOperator.partial(
        task_id="delete_partition",
        conn_id="cratedb_connection",
        sql="DELETE from keywords_random_ip_ttl_test_min WHERE TIMESTAMP > 1699170005565;",
    )


ttl_delete_test()
