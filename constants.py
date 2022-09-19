import os
import urllib.request

from aws_cdk import App, Stack, Environment
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2

# Update EC2 Key Pair
DEV_KEY_PAIR = "mwaa-openlineage"

# Update client external ip to access OpenLineage UI
EXTERNAL_IP = "255.255.255.255"

CDK_APP_NAME = "cdkdl"
CDK_APP_PYTHON_VERSION = "3.7"

# pylint: disable=line-too-long
GITHUB_CONNECTION_ARN = "CONNECTION_ARN"
GITHUB_OWNER = "OWNER"
GITHUB_REPO = "REPO"
GITHUB_TRUNK_BRANCH = "TRUNK_BRANCH"

DEV_ENV = Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]
)

DEV_CLOUD9_INSTANCE_TYPE = "t3.xlarge"
DEV_CLOUD9_IMAGE_ID = "amazonlinux-2-x86_64"
DEV_EMR_NAME=f"{CDK_APP_NAME}-dev"
DEV_EMR_RELEASE_LABEL = "6.0.0"
DEV_EMR_CORE_INSTANCE_COUNT = 1
DEV_EMR_CORE_INSTANCE_TYPE = "m4.large"
DEV_EMR_MASTER_INSTANCE_COUNT = 1
DEV_EMR_MASTER_INSTANCE_TYPE = "m4.large"
DEV_EMR_NAME=f"{CDK_APP_NAME}-dev"
DEV_GLUE_DB=f"{CDK_APP_NAME}-redshift"
DEV_LINEAGE_INSTANCE = ec2.InstanceType("t2.xlarge")

DEV_MWAA_ENV_CLASS = "mw1.small"
DEV_MWAA_ENV_NAME=f"{CDK_APP_NAME}-dev"
# DEV_MWAA_PLUGINS_VERSION = "H_YfcX5gK_gBfHFbqoOUq4pDgt4DZwfD"
# DEV_MWAA_REQUIREMENTS_VERSION = "N0MsJPsONyYtd6o_afTskMHrvG78vutT"
DEV_OPENLINEAGE_NAMESPACE=f"{CDK_APP_NAME}-dev"
DEV_MWAA_REPO_DAG_NAME=f"{CDK_APP_NAME}-dev-mwaa-repo-dag"

DEV_PERMISSIONS = "IAM"
DEV_REDSHIFT_CLUSTER_TYPE = "multi-node"
DEV_REDSHIFT_MASTER_USERNAME = f"{CDK_APP_NAME}_user"
DEV_REDSHIFT_DB_NAME=f"{CDK_APP_NAME}-dev"
DEV_REDSHIFT_NUM_NODES= 2
DEV_REDSHIFT_NODE_TYPE= "ra3.4xlarge"
DEV_REDSHIFT_NAMESPACE=f"{CDK_APP_NAME}ns"
DEV_REDSHIFT_WORKGROUP=f"{CDK_APP_NAME}wg"


PIPELINE_ENV = Environment(account="222222222222", region="eu-west-1")

PROD_ENV = Environment(account="333333333333", region="eu-west-1")
PROD_API_LAMBDA_RESERVED_CONCURRENCY = 10
PROD_DATABASE_DYNAMODB_BILLING_MODE = dynamodb.BillingMode.PROVISIONED
