# from: https://amazon-mwaa-for-analytics.workshop.aws/en/workshop/m1-processing.html

#from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator

from datetime import timedelta  
import airflow  
from airflow import DAG  
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor  
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator  
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator  
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor  
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator  

# Custom Operators deployed as Airflow plugins
from awsairflowlib.operators.aws_glue_job_operator import AWSGlueJobOperator
from awsairflowlib.operators.aws_glue_crawler_operator import AWSGlueCrawlerOperator
from awsairflowlib.operators.aws_copy_s3_to_redshift import CopyS3ToRedshiftOperator  

# replace airflow DAG with openlineage DAG
# import openlineage
# from openlineage.airflow import DAG

def print_hello():
    # show installed packages
    print("pip list", os.system("pip list"))
    print("dir(openlinage)", dir(openlineage))
    return "Hello Wolrd"


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

# s3 sensor
s3_sensor = S3PrefixSensor(  
  task_id='s3_sensor',  
  bucket_name=S3_BUCKET_NAME,  
  prefix='data/raw/green',  
  dag=dag  
)

# need to have an existing glue crawler
# glue crawler
glue_crawler = AWSGlueCrawlerOperator(
    task_id="glue_crawler",
    crawler_name='airflow-workshop-raw-green-crawler',
    iam_role_name='AWSGlueServiceRoleDefault',
    dag=dag)

# glue job
glue_task = AWSGlueJobOperator(  
    task_id="glue_task",  
    job_name='nyc_raw_to_transform',  
    iam_role_name='AWSGlueServiceRoleDefault',  
    dag=dag) 


# dag flow
s3_sensor >> glue_crawler >> glue_task >> cluster_creator >> step_adder >> step_checker >> cluster_remover >> copy_agg_to_redshift

