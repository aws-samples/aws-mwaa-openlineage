import requests
import json
import boto3

client = boto3.client("glue")

url = f"http://ec2-107-21-174-143.compute-1.amazonaws.com:5000/api/v1"
# resp = requests.get(url)
mydict = {
    "version": "0",
    "id": "6beeac3f-e09c-b403-0912-c3e55902f601",
    "detail-type": "Glue Data Catalog Table State Change",
    "source": "aws.glue",
    "account": "820406222077",
    "time": "2021-11-15T07:38:23Z",
    "region": "us-east-1",
    "resources": [
        "arn:aws:glue:us-east-1:820406222077:table/cdkdl-dev/cdkdl_dev_foundationstoragef3_s3bucketraw88473b86_cxz3i1e716d3"
    ],
    "detail": {
        "databaseName": "cdkdl-dev",
        "changedPartitions": ["[nyctaxi]"],
        "typeOfChange": "BatchCreatePartition",
        "tableName": "cdkdl_dev_foundationstoragef3_s3bucketraw88473b86_cxz3i1e716d3",
    },
}
print(json.dumps(mydict))

# get the glue table definition
response = client.get_table(
    DatabaseName="cdkdl-dev",
    Name="cdkdl_dev_foundationstoragef3_s3bucketraw88473b86_cxz3i1e716d3",
)
print(response)
# get the glue table version definition
response = client.get_table_version(
    DatabaseName="cdkdl-dev",
    TableName="cdkdl_dev_foundationstoragef3_s3bucketraw88473b86_cxz3i1e716d3",
    VersionId="0"
)
print(x)
# print(resp.status_code)
# get namespaces
# curl http://ec2-107-21-174-143.compute-1.amazonaws.com:5000/api/v1/namespaces
# create a source
# curl http://ec2-107-21-174-143.compute-1.amazonaws.com:5000/api/v1/sources
# get namespaces

resp = requests.get(f"{url}/namespaces")
print(resp.status_code)
print(resp.reason)
print(resp.content)

namespace = "cdkdl-dev"
dataset = "mydataset1"

payload = {
    "type": "DB_TABLE",
    "physicalName": "public.mytable",
    "sourceName": "my-source",
    "fields": [
        {"name": "a", "type": "INTEGER"},
        {"name": "b", "type": "TIMESTAMP"},
        {"name": "c", "type": "INTEGER"},
        {"name": "d", "type": "INTEGER"},
    ],
    "description": "My first dataset!",
}

resp = requests.put(
    f"{url}/namespaces/{namespace}/datasets/{dataset}",
    json=payload,
    headers={"Content-Type": "application/json"},
    timeout=5,
)

print(resp.status_code)
print(resp.reason)
print(resp.content)


{
    "HOSTNAME": "7419b34b9df7",
    "LOAD_EX": "n",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "PWD": "/usr/local/airflow",
    "AIRFLOW__CORE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow",
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    "SHLVL": "1",
    "HOME": "/usr/local/airflow",
    "AIRFLOW__CORE__FERNET_KEY": "pqUP0OBXiIdJ1Uav3Q2tUFBNgCIqigQ_bk8Ap6tEpDc=",
    "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
    "AIRFLOW_HOME": "/usr/local/airflow",
    "EXECUTOR": "Local",
    "_": "/usr/local/bin/airflow",
    "LC_CTYPE": "C.UTF-8",
    "AIRFLOW__LINEAGE__BACKEND": "openlineage.lineage_backend.OpenLineageBackend",
    "AIRFLOW_VAR_OPENLINEAGE_URL": "http://ec2-107-21-174-143.compute-1.amazonaws.com:5000",
    "AIRFLOW_VAR_OPENLINEAGE_NAMESPACE": "cdkdl-dev",
    "AIRFLOW_CONN_REDSHIFT_CONNECTOR": "postgres://cdkdl_user:6YvXgZo3YYrOMYE0AZ9KD9myoANfaOo0@redshiftcluster-0eipe8tlxv32.cswv0g7cq8og.us-east-1.redshift.amazonaws.com:5439/dev",
    "AIRFLOW_CTX_DAG_OWNER": "airflow",
    "AIRFLOW_CTX_DAG_ID": "hello_world",
    "AIRFLOW_CTX_TASK_ID": "hello_task",
    "AIRFLOW_CTX_EXECUTION_DATE": "2021-11-09T20:09:28.299683+00:00",
    "AIRFLOW_CTX_DAG_RUN_ID": "manual__2021-11-09T20:09:28.299683+00:00",
}

YY/MM/DD HH:mm:ss INFO SparkContext: Registered listener io.openlineage.spark.agent.OpenLineageSparkListener
YY/MM/DD HH:mm:ss INFO OpenLineageContext: Init OpenLineageContext: Args: ArgumentParser(host=https://YOURHOST, version=1, namespace=YOURNAMESPACE, jobName=default, parentRunId=null, apiKey=Optional.empty) URI: https://YOURHOST/api/1/lineage
YY/MM/DD HH:mm:ss INFO AsyncEventQueue: Process of event SparkListenerApplicationStart(Databricks Shell,Some(app-XXX-0000),YYYY,root,None,None,None) by listener OpenLineageSparkListener took Xs.

21/11/22 18:48:48 INFO SparkContext: Registered listener io.openlineage.spark.agent.OpenLineageSparkListener
21/11/22 18:48:49 INFO OpenLineageContext: Init OpenLineageContext: Args: ArgumentParser(host=http://ec2-54-227-194-66.compute-1.amazonaws.com:5000, version=v1, namespace=spark_integration, jobName=default, parentRunId=null, apiKey=Optional.empty) URI: http://ec2-54-227-194-66.compute-1.amazonaws.com:5000/api/v1/lineage
21/11/22 18:48:49 INFO AsyncEventQueue: Process of event SparkListenerApplicationStart(nyc-taxi-raw-stage,Some(spark-application-1637606927106),1637606926281,spark,None,None,None) by listener OpenLineageSparkListener took 1.092252643s.
