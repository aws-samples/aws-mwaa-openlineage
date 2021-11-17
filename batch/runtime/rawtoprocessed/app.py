"""
Process a nyc-taxi csv file into parquet
Write the lineage into openlineage
Need a glue network connection to the ec2 instance
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "GLUE_DATABASE",
        "S3_BUCKET_RAW",
        "S3_BUCKET_PROCESSED",
        "spark.openlineage.host",
        "spark.openlineage.namespace",
        "spark.jars.packages",
        "spark.extraListeners",
        #"enable-continuous-cloudwatch-log",
    ],
)

sc = SparkContext()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# show input args
logger.info("info message")
logger.warn("warn message")
logger.error("error message")
print("print message")

logger.info("args")
logger.info(args["GLUE_DATABASE"])
logger.info(args["S3_BUCKET_RAW"])
logger.info(args["S3_BUCKET_PROCESSED"])

# source
s3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://cdkdl-dev-foundationstoragef3-s3bucketraw88473b86-cxz3i1e716d3/nyctaxi/"
        ],
        "recurse": False,
    },
    transforeation_ctx="s3bucket_node1",
)

# apply mapping
applymapping_node2 = ApplyMapping.apply(
    frame=s3bucket_node1,
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
    transformation_ctx="applymapping_node2",
)

# resolve choice
resolvechoice_node3 = ResolveChoice.apply(
    frame=applymapping_node2,
    choice="make_struct",
    transformation_ctx="resolvechoice_node3",
)

# drop nulls
dropnullfields_node4 = DropNullFields.apply(
    frame=resolvechoice_node3, transformation_ctx="dropnullfields_node4"
)

# sink
s3bucket_node5 = glueContext.write_dynamic_frame.from_options(
    frame=dropnullfields_node4,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://cdkdl-dev-foundationstor-s3bucketprocessed33f6f27-k5thz0k95quv/nyctaxi/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="s3bucket_node5",
)

# commit :)
job.commit()
