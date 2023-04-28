import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job as GlueJob
import boto3
from openlineage.client import OpenLineageClient
from openlineage.client.facet import SymlinksDatasetFacet, SymlinksDatasetFacetIdentifiers
from openlineage.client.run import Dataset, RunEvent, RunState, Run, Job
import datetime

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "TempDir",
    "redshift_conn_string",
    "redshift_user",
    "redshift_password_secret_id",
    "output_bucket",
    "redshift_cluster_role",
    "lineage_api_url"
    ])

boto_session = boto3.Session()
lineage_client = OpenLineageClient(url=args["lineage_api_url"])

sc = SparkContext(appName="example_job")
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
glue_job = GlueJob(glueContext)
glue_job.init(args['JOB_NAME'], args)

secrets = boto_session.client('secretsmanager')

redshift_password = secrets.get_secret_value(SecretId=args["redshift_password_secret_id"])["SecretString"]

df = spark \
    .read \
    .format("jdbc") \
    .option("url", args["redshift_conn_string"]) \
    .option("dbtable", "public.fact_sales") \
    .option("aws_iam_role", args["redshift_cluster_role"]) \
    .option("user", args["redshift_user"]) \
    .option("password", redshift_password) \
    .load() \
    .limit(10)
fact_sales_symlink_id = SymlinksDatasetFacetIdentifiers(
    namespace="redshift://cdkdlwg.us-west-2:5439",
    name="cdkdl-dev.public.fact_sales",
    type="table"
)
fact_sales_symlink = SymlinksDatasetFacet(identifiers=[fact_sales_symlink_id])
fact_sales_dataset = Dataset(
    namespace="redshift://cdkdlwg.589336247955.us-west-2.redshift-serverless.amazonaws.com:5439/cdkdl-dev",
    name="public.fact_sales",
    facets={
        "symlinks": fact_sales_symlink
    }
)

run = Run(
    runId="f7e7f408-b112-4fc0-a463-d27e05a25294"
)

job = Job(namespace="default", name="example_job.execute_insert_into_hadoop_fs_relation_command")

run_event = RunEvent(
    eventType=RunState.RUNNING,
    eventTime=datetime.datetime.now().isoformat(),
    run=run,
    job=job,
    producer="https://github.com/OpenLineage/OpenLineage/tree/0.22.0/integration/spark",
    inputs=[fact_sales_dataset]
)
lineage_client.emit(run_event)

print("writing")
df.write.mode("overwrite").parquet(f"s3://{args['output_bucket']}/test/fact_sales_filtered")

print("done")

glue_job.commit()
