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
from airflow.models import Variable
from airflow.configuration import conf


def print_hello():

    # show the conf
    logging.info("Show the conf")
    logging.info(
        f"AIRFLOW__LINEAGE__BACKEND: {conf.get(section='LINEAGE', key='BACKEND')}"
    )
    logging.info(
        f"AIRFLOW__CORE__LAZY_LOAD_PLUGINS: {conf.get(section='CORE', key='LAZY_LOAD_PLUGINS')}"
    )

    # show the variables
    logging.info("Show the variables")
    openlineage_url = Variable.get('OPENLINEAGE_URL', default_var='')
    logging.info(f"OPENLINEAGE_URL: {openlineage_url}")
    logging.info(
        f"OPENLINEAGE_NAMESPACE: {Variable.get('OPENLINEAGE_NAMESPACE', default_var='')}"
    )

    # url for the namespaces call
    url = f'{openlineage_url}/api/v1/namespaces'
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
    task_id="hello_task", python_callable=print_hello, provide_context=True, dag=dag
)

dummy_operator >> hello_operator
