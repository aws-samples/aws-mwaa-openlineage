#!/usr/bin/env python3
from aws_cdk import App, Stack, Aspects, Environment
from aws_cdk import pipelines, Aws

# The AWS CDK application entry point
import constants

# get stacks
from storage.infrastructure import S3
from governance.infrastructure import DataZone
from orchestration.infrastructure import MWAA
from consume.infrastructure import Redshift

app = App()
#This enables security validation through cdk_nag
#Aspects.of(app).add(AwsSolutionsChecks(verbose=True))



s3 = S3(app,
        "vpc-s3",
        EXTERNAL_IP=constants.EXTERNAL_IP,
        DEV_GLUE_DB=constants.DEV_GLUE_DB,
        CDK_APP_NAME=constants.CDK_APP_NAME,
        env=constants.DEV_ENV)

dz = DataZone(
    app,
    "datazone",
    env=constants.DEV_ENV,
    S3_BUCKET_RAW=s3.S3_BUCKET_RAW
)

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


app.synth()
