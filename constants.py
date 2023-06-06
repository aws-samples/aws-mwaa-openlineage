import os
import urllib.request

from aws_cdk import App, Stack, Environment
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2


# Update client external ip to access OpenLineage UI
EXTERNAL_IP = "255.255.255.255"

DEV_ENV = Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]
)

CDK_APP_NAME = "dl"
CDK_APP_PYTHON_VERSION = "3.7"

DEV_GLUE_RAW_DB=f"{CDK_APP_NAME}_raw_db"
DEV_GLUE_CURATED_DB=f"{CDK_APP_NAME}_curated_db"
DEV_LINEAGE_INSTANCE = ec2.InstanceType("t2.xlarge")

DEV_MWAA_ENV_CLASS = "mw1.small"
DEV_MWAA_ENV_NAME=f"{CDK_APP_NAME}-dev"
DEV_OPENLINEAGE_NAMESPACE=f"{CDK_APP_NAME}-dev"
DEV_MWAA_REPO_DAG_NAME=f"{CDK_APP_NAME}-dev-mwaa-repo-dag"
DEV_MWAA_ENV_VERSION = "2.4.3"

DEV_PERMISSIONS = "IAM"
DEV_REDSHIFT_CLUSTER_TYPE = "multi-node"
DEV_REDSHIFT_MASTER_USERNAME = f"{CDK_APP_NAME}_user"
DEV_REDSHIFT_DB_NAME=f"{CDK_APP_NAME}-dev"
DEV_REDSHIFT_NUM_NODES= 2
DEV_REDSHIFT_NODE_TYPE= "ra3.4xlarge"
DEV_REDSHIFT_NAMESPACE=f"{CDK_APP_NAME}ns"
DEV_REDSHIFT_WORKGROUP=f"{CDK_APP_NAME}wg"


