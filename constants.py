import os

from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import core as cdk
from aws_cdk import aws_ec2 as ec2

CDK_APP_NAME = "cdkdl"
CDK_APP_PYTHON_VERSION = "3.7"

# pylint: disable=line-too-long
GITHUB_CONNECTION_ARN = "CONNECTION_ARN"
GITHUB_OWNER = "OWNER"
GITHUB_REPO = "REPO"
GITHUB_TRUNK_BRANCH = "TRUNK_BRANCH"

DEV_ENV = cdk.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"]
)
DEV_KEY_PAIR = "newKeyPair"
DEV_MARQUEZ_INSTANCE = ec2.InstanceType("t2.xlarge")
DEV_MWAA_ENV_CLASS = "mw1.small"
DEV_MWAA_PLUGINS_VERSION = "UTzoVKUtGdPqtp2RzwnZvZ1htPJRlswP"
DEV_MWAA_REQUIREMENTS_VERSION = "HitsjRk2yoY8T3BkNOZdcSzBSUK48Ayi"
DEV_PERMISSIONS = "IAM"
DEV_LF_ADMIN_USER = "mcgregf-dev1"

PIPELINE_ENV = cdk.Environment(account="222222222222", region="eu-west-1")

PROD_ENV = cdk.Environment(account="333333333333", region="eu-west-1")
PROD_API_LAMBDA_RESERVED_CONCURRENCY = 10
PROD_DATABASE_DYNAMODB_BILLING_MODE = dynamodb.BillingMode.PROVISIONED
