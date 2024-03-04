#!/usr/bin/env python3
from aws_cdk import App, Stack, Aspects, Environment
from aws_cdk import pipelines, Aws

# The AWS CDK application entry point
import constants
# from deployment import CDKDataLake
from cdk_nag import AwsSolutionsChecks, NagSuppressions
# get stacks
from storage.infrastructure import S3
from governance.infrastructure import Marquez
from orchestration.infrastructure import MWAA
from consume.infrastructure import Redshift

app = App()
#This enables security validation through cdk_nag
#Aspects.of(app).add(AwsSolutionsChecks(verbose=True))

s3 = S3(app,
        "vpc-s3",
        EXTERNAL_IP=constants.EXTERNAL_IP,
        DEV_GLUE_DB=constants.DEV_GLUE_DB,
        env=constants.DEV_ENV,)
NagSuppressions.add_stack_suppressions(
    stack=s3,
    suppressions=[{
        "id": "AwsSolutions-EC23",
        "reason": "No security group have allow inbound to any IP"
    },{
        "id": "AwsSolutions-IAM4",
        "reason": "Allow managed policies to be used"
    },{
        "id": "AwsSolutions-IAM5",
        "reason": "Using CDK IAM allow permissions."
    },{
        "id": "AwsSolutions-GL1",
        "reason": "Using Crawler default setting."
    },{
        "id": "AwsSolutions-VPC7",
        "reason": "No VPC Flow log enabled. Default behaviour."
    },{
        "id": "AwsSolutions-L1",
        "reason": "Referencing default runtime used by CDK for Python3.8"
    },{
        "id": "AwsSolutions-S1",
        "reason": "Bucket will be used to store server access logs."
    },
    ])

marquez = Marquez(
    app,
    "marquez",
    VPC=s3.VPC,
    LINEAGE_INSTANCE=constants.DEV_LINEAGE_INSTANCE,
    OPENLINEAGE_NAMESPACE=constants.DEV_OPENLINEAGE_NAMESPACE,
    OPENLINEAGE_SG=s3.OPENLINEAGE_SG,
    env=constants.DEV_ENV,
)
NagSuppressions.add_stack_suppressions(
    stack=marquez,
    suppressions=[{
        "id": "AwsSolutions-EC29",
        "reason": "No termination protection to enable cleanup of resources."
    },{
        "id": "AwsSolutions-IAM4",
        "reason": "Allow managed policies to be used"
    },{
        "id": "AwsSolutions-SMG4",
        "reason": "Temporary solution no secret rotation. Default behaviour."
    }
    ])

redshift = Redshift(
    app,
    "redshift",
    VPC=s3.VPC,
    S3_BUCKET_RAW=s3.S3_BUCKET_RAW,
    REDSHIFT_DB_NAME=constants.DEV_REDSHIFT_DB_NAME,
    REDSHIFT_NAMESPACE=constants.DEV_REDSHIFT_NAMESPACE,
    REDSHIFT_WORKGROUP=constants.DEV_REDSHIFT_WORKGROUP,
    REDSHIFT_MASTER_USERNAME=constants.DEV_REDSHIFT_MASTER_USERNAME,
    REDSHIFT_SG=s3.REDSHIFT_SG,
    env=constants.DEV_ENV,
)

NagSuppressions.add_stack_suppressions(
    stack=redshift,
    suppressions=[{
        "id": "AwsSolutions-IAM5",
        "reason": "Using CDK IAM allow permissions."
    },{
        "id": "AwsSolutions-IAM4",
        "reason": "Allow managed policies to be used"
    },{
        "id": "AwsSolutions-SMG4",
        "reason": "Temporary solution no secret rotation. Default behaviour."
    }
    ])

mwaa = MWAA(
    app,
    "mwaa",
    VPC=s3.VPC,
    MWAA_ENV_NAME=constants.DEV_MWAA_ENV_NAME,
    MWAA_ENV_CLASS=constants.DEV_MWAA_ENV_CLASS,
    MWAA_ENV_VERSION=constants.DEV_MWAA_ENV_VERSION,
    MWAA_DEPLOY_FILES=True,
    MWAA_REPO_DAG_NAME=constants.DEV_MWAA_REPO_DAG_NAME,
    AIRFLOW_SG=s3.AIRFLOW_SG,
    env=constants.DEV_ENV,
)
mwaa.add_dependency(redshift)
NagSuppressions.add_stack_suppressions(
    stack=mwaa,
    suppressions=[{
        "id": "AwsSolutions-KMS5",
        "reason": "Temporary solution no KMS rotation. Default behaviour."
    },{
        "id": "AwsSolutions-IAM4",
        "reason": "Allow managed policies to be used"
    },{
        "id": "AwsSolutions-IAM5",
        "reason": "Using CDK IAM allow permissions."
    },{
        "id": "AwsSolutions-L1",
        "reason": "Referencing default runtime used by CDK for Python3.8"
    },{
        "id": "AwsSolutions-S1",
        "reason": "No server access logs. Only contain DAG code and metadata."
    },
    ])

app.synth()
