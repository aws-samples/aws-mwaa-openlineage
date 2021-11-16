import os
import logging
from datetime import datetime
import requests

# airflow env setup
from openlineage.airflow.dag import DAG

# airflow imports
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.configuration import conf


def print_hello():

    # show the env
    logging.info("Show the env")
    logging.info(os.environ)

    # show the conf
    logging.info("Show the conf")
    logging.info(
        f"AIRFLOW__CORE__LAZY_LOAD_PLUGINS: {conf.get(section='CORE', key='LAZY_LOAD_PLUGINS')}"
    )
    logging.info(
        f"AIRFLOW__LINEAGE__BACKEND: {conf.get(section='LINEAGE', key='BACKEND')}"
    )

    # show the variables
    logging.info("Show the variables")
    logging.info(f"OPENLINEAGE_URL: {Variable.get('OPENLINEAGE_URL', default_var='')}")
    logging.info(
        f"OPENLINEAGE_NAMESPACE: {Variable.get('OPENLINEAGE_NAMESPACE', default_var='')}"
    )

    # set the url for the namespaces call
    url = f"{Variable.get('OPENLINEAGE_URL', default_var='')}/api/v1/namespaces"
    time_out = 5

    # send the request
    try:
        logging.info(f"Sending request GET to {url}")
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
