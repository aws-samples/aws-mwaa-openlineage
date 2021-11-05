import os
import logging
from datetime import datetime
import requests

# replace airflow DAG with openlineage DAG
# from airflow import DAG
from openlineage.airflow.dag import DAG

# from openlineage.airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():

    logging.info(f'AIRFLOW_LINEAGE_BACKEND: {os.getenv("AIRFLOW__LINEAGE__BACKEND")}')
    logging.info(f'OPENLINEAGE_URL: {os.getenv("OPENLINEAGE_URL")}')
    logging.info(f'OPENLINEAGE_NAMESPACE: {os.getenv("OPENLINEAGE_NAMESPACE")}')

    url = f'{os.getenv("OPENLINEAGE_URL")}/api/v1/namespaces'
    time_out = 5

    # send the request
    try:
        resp = requests.get(url, timeout=time_out)
        logging.info(f"resp.status_code: {resp.status_code}")
        logging.info(f"resp.reason: {resp.reason}")
        logging.info(f"resp.content: {resp.content}")

    except requests.Timeout:
        logging.warning(f"Request to {url} timed out after {time_out} seconds")

    return "Hello world"


dag = DAG(
    "hello_world",
    description="Hello world example",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

dummy_operator = DummyOperator(task_id="dummy_task", retries=3, dag=dag)

hello_operator = PythonOperator(
    task_id="hello_task", python_callable=print_hello, dag=dag
)

dummy_operator >> hello_operator
