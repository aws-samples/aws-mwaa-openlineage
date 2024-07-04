
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_target_bucket', 'table_name','raw_db','curated_db'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


s3bucket = args['s3_target_bucket']
sourcedatabaseName = args['raw_db']
targetdatabaseName = args['curated_db']
tableName = args['table_name']


datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database=sourcedatabaseName, 
    table_name=tableName, 
    transformation_ctx="datasource0"
)

s3output = glueContext.getSink(
  path=f"s3://{s3bucket}/tickit/{tableName}/",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase=targetdatabaseName, catalogTableName=tableName
)
s3output.setFormat("glueparquet")
s3output.writeFrame(datasource0)
job.commit()