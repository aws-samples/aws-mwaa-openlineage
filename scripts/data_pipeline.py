import os
import logging
from datetime import datetime
from datetime import timedelta
import requests

# plugin for env vars
from env_var_plugin import EnvVarPlugin

# airflow modules
import airflow

# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor

# from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
# from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
# from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

# Custom Operators deployed as Airflow plugins
# from awsairflowlib.operators.aws_glue_job_operator import AWSGlueJobOperator
# from awsairflowlib.operators.aws_glue_crawler_operator import AWSGlueCrawlerOperator
# from awsairflowlib.operators.aws_copy_s3_to_redshift import CopyS3ToRedshiftOperator

# replace airflow DAG with openlineage DAG
from openlineage.airflow.dag import DAG

# Custom Operators deployed as Airflow plugins
# from awsairflowlib.operators.aws_glue_job_operator import AWSGlueJobOperator
# from awsairflowlib.operators.aws_glue_crawler_operator import AWSGlueCrawlerOperator
# from awsairflowlib.operators.aws_copy_s3_to_redshift import CopyS3ToRedshiftOperator


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


# the dag
dag = DAG(
    "data_pipeline",
    description="Data pipeline example",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

s3_sensor = S3PrefixSensor(
    task_id="s3_sensor", bucket_name=S3_BUCKET_NAME, prefix="data/raw/green", dag=dag
)


# # glue_crawler = AWSGlueCrawlerOperator(
#     task_id="glue_crawler",
#    crawler_name='airflow-workshop-raw-green-crawler',
#   iam_role_name='AWSGlueServiceRoleDefault',
#  dag=dag)

hello_operator = PythonOperator(
    task_id="hello_task", python_callable=print_hello, dag=dag
)

hello_operator >> s3_sensor
