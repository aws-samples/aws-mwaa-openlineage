#!/usr/bin/env python3
from aws_cdk import App, Stack, Aspects
from aws_cdk import pipelines

# The AWS CDK application entry point
import constants
# from deployment import CDKDataLake
from cdk_nag import AwsSolutionsChecks
# get stacks
from storage.infrastructure import S3
from governance.infrastructure import Lineage
from orchestration.infrastructure import MWAA
from consume.infrastructure import Redshift

app = App()
#This enables security validation through cdk_nag
#Aspects.of(app).add(AwsSolutionsChecks(verbose=True))

s3 = S3(app,
        "s3",
        EXTERNAL_IP=constants.EXTERNAL_IP)

lineage = Lineage(
    app,
    "lineage",
    VPC=s3.VPC,
    LINEAGE_INSTANCE=constants.DEV_LINEAGE_INSTANCE,
    KEY_PAIR=constants.DEV_KEY_PAIR,
    OPENLINEAGE_SG=s3.OPENLINEAGE_SG,
)
redshift = Redshift(
    app,
    "redshift",
    VPC=s3.VPC,
    REDSHIFT_DB_NAME=constants.DEV_REDSHIFT_DB_NAME,
    REDSHIFT_NAMESPACE=constants.DEV_REDSHIFT_NAMESPACE,
    REDSHIFT_WORKGROUP=constants.DEV_REDSHIFT_WORKGROUP,
    REDSHIFT_MASTER_USERNAME=constants.DEV_REDSHIFT_MASTER_USERNAME,
    REDSHIFT_SG=s3.REDSHIFT_SG,
)

mwaa = MWAA(
    app,
    "mwaa",
    VPC=s3.VPC,
    MWAA_ENV_NAME=constants.DEV_MWAA_ENV_NAME,
    MWAA_ENV_CLASS=constants.DEV_MWAA_ENV_CLASS,
    MWAA_DEPLOY_FILES=True,
    MWAA_REPO_DAG_NAME=constants.DEV_MWAA_REPO_DAG_NAME,
    AIRFLOW_SG=s3.AIRFLOW_SG,
)
# Production pipeline
# Pipeline(app, f"{constants.CDK_APP_NAME}-pipeline", env=constants.PIPELINE_ENV)

app.synth()
