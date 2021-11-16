import random

from openlineage.airflow.dag import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "datascience",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["datascience@example.com"],
    "postgres_conn_id": "REDSHIFT_CONNECTOR",
}

dag = DAG(
    "counter",
    schedule_interval="*/1 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    description="DAG that generates a new count value between 1-10.",
)

t1 = PostgresOperator(
    task_id="if_not_exists",
    sql="""
    CREATE TABLE IF NOT EXISTS counts (
      value INTEGER
    );""",
    dag=dag,
)

t2 = PostgresOperator(
    task_id="inc",
    sql="""
    INSERT INTO counts (value)
         VALUES (%(value)s)
    """,
    parameters={"value": random.randint(1, 10)},
    dag=dag,
)

t1 >> t2
