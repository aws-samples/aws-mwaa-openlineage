"""
Process a nyc-taxi csv file into parquet
Write the lineage into openlineage
Need a glue network connection to the ec2 instance
"""

import sys
#import requests
#from awsglue.transforms import *
#from awsglue.utils import getResolvedOptions
#from awsglue.context import GlueContext
#from awsglue.job import Job
#from pyspark.context import SparkContext, SparkConf

from pyspark.sql import SparkSession

# get the args
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        # glue database
        "GLUE_DATABASE",
        # source bucket
        "S3_BUCKET_RAW",
        # desintation bucket
        "S3_BUCKET_STAGE",
        # openlineage host
        "OPENLINEAGE_HOST",
    ],
)
# set the openlineage url for spark
spark_openlineage_url = (
    f'{args["OPENLINEAGE_HOST"]}/api/v1/namespaces/spark_integration/'
)

# create the spark session
spark = (SparkSession.builder.master('local').appName('openlineage_spark_test')
             
             # Install and set up the OpenLineage listener
             .config('spark.jars.packages', 'io.openlineage:openlineage-spark:0.3.+')
             .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
             .config('spark.openlineage.host', args["OPENLINEAGE_HOST"])
             .config('spark.openlineage.namespace', 'spark_integration')
             
             # Configure the Google credentials and project id
             #.config('spark.executorEnv.GCS_PROJECT_ID', project_id)
             #.config('spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS', '/home/jovyan/notebooks/gcs/bq-spark-demo.json')
             #.config('spark.hadoop.google.cloud.auth.service.account.enable', 'true')
             #.config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', credentials_file)
             #.config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
             #.config('spark.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS')
             #.config("spark.hadoop.fs.gs.project.id", project_id)
             .getOrCreate())

# set spark conf, app name is used by lineage
conf = SparkConf().setAppName("nyc-taxi-raw-stage")
# SparkListener can be referenced as a plain Spark Listener implementation.
conf.set("spark.jars.packages", "io.openlineage:openlineage-spark:0.3.1")
conf.set("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
conf.set("spark.openlineage.url", spark_openlineage_url)
conf.set("parentProject", "nyc-taxi-stuff")


# build the spark context
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# print the args to output logs
logger.info("args")
logger.info(f"GLUE_DATABASE: {args['GLUE_DATABASE']}")
logger.info(f'S3_BUCKET_RAW: {args["S3_BUCKET_RAW"]}')
logger.info(f'S3_BUCKET_STAGE: {args["S3_BUCKET_STAGE"]}')
logger.info(f'OPENLINEAGE_HOST: {args["OPENLINEAGE_HOST"]}')
logger.info(f"spark.openlineage.url: {spark_openlineage_url}")


# url for the namespaces call
url = f'{args["OPENLINEAGE_HOST"]}/api/v1/namespaces'
time_out = 5

# send the request
try:
    resp = requests.get(url, timeout=time_out)
    logger.info(f"resp.status_code: {resp.status_code}")
    logger.info(f"resp.reason: {resp.reason}")
    logger.info(f"resp.content: {resp.content}")

except requests.Timeout:
    logger.warn(f"Request to {url} timed out after {time_out} seconds")

# not using glue
# read df from csv from raw
# df = (
#    spark.read.format("csv")
#    .options(
#        header="true", inferSchema="true", delimiter=",", parentProject=args["JOB_NAME"]
#    )
#    .load(f"s3a://{args['S3_BUCKET_RAW']}/nyctaxi/")
# )

# write the df back to stage
# df.write.mode("overwrite").option("parentProject", args["JOB_NAME"]).parquet(
#    f"s3://{args['S3_BUCKET_STAGE']}/nyctaxi/nyctaxi.parquet"
# )

# execute the transforms
# source
df_csv = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"s3://{args['S3_BUCKET_RAW']}/nyctaxi/"],
        "recurse": False,
    },
    # transforeation_ctx="s3bucket_node1",
)

# apply mapping
df_mapped = ApplyMapping.apply(
    frame=df_csv,
    mappings=[
        ("VendorID", "long", "VendorID", "long"),
        ("lpep_pickup_datetime", "string", "lpep_pickup_datetime", "string"),
        ("lpep_dropoff_datetime", "string", "lpep_dropoff_datetime", "string"),
        ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"),
        ("RatecodeID", "long", "RatecodeID", "long"),
        ("PULocationID", "long", "PULocationID", "long"),
        ("DOLocationID", "long", "DOLocationID", "long"),
        ("passenger_count", "long", "passenger_count", "long"),
        ("trip_distance", "double", "trip_distance", "double"),
        ("fare_amount", "choice", "fare_amount", "choice"),
        ("extra", "choice", "extra", "choice"),
        ("mta_tax", "choice", "mta_tax", "choice"),
        ("tip_amount", "choice", "tip_amount", "choice"),
        ("tolls_amount", "choice", "tolls_amount", "choice"),
        ("ehail_fee", "null", "ehail_fee", "null"),
        ("improvement_surcharge", "choice", "improvement_surcharge", "choice"),
        ("total_amount", "choice", "total_amount", "choice"),
        ("payment_type", "long", "payment_type", "long"),
        ("trip_type", "long", "trip_type", "long"),
        ("congestion_surcharge", "choice", "congestion_surcharge", "choice"),
    ],
    # transformation_ctx="applymapping_node2",
)

# resolve choice
df_resolved = ResolveChoice.apply(
    frame=df_mapped,
    choice="make_struct",
    # transformation_ctx="resolvechoice_node3",
)

# drop nulls
df_dropped = DropNullFields.apply(
    frame=df_resolved,  # , transformation_ctx="dropnullfields_node4"
    # parentProject="nyc-tax-raw-stage-job",
)

# sink
glueContext.write_dynamic_frame.from_options(
    frame=df_dropped,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": f"s3://{args['S3_BUCKET_STAGE']}/nyctaxi/",
    },
    format_options={"compression": "snappy"},
    # transformation_ctx="s3bucket_node5",
)

# Script generated for node S3 bucket
# S3bucket_node3 = glueContext.getSink(
#    path=f"s3://{args['S3_BUCKET_STAGE']}/nyctaxi/",
#    connection_type="s3",
#    updateBehavior="UPDATE_IN_DATABASE",
#    partitionKeys=[],
#    compression="snappy",
#    enableUpdateCatalog=True,
# )
# S3bucket_node3.setCatalogInfo(
#    catalogDatabase="cdkdl-dev", catalogTableName="nyc-taxi-stage"
# )
# S3bucket_node3.setFormat("glueparquet")
# S3bucket_node3.writeFrame(df_dropped)

# commit :)
job.commit()
