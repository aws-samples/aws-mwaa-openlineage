#!/usr/bin/env python3
from aws_cdk import App, Stack, Aspects, Environment
from aws_cdk import pipelines, Aws

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
        EXTERNAL_IP=constants.EXTERNAL_IP,
        DEV_GLUE_DB=constants.DEV_GLUE_DB,
        env=constants.DEV_ENV,)

lineage = Lineage(
    app,
    "lineage",
    VPC=s3.VPC,
    LINEAGE_INSTANCE=constants.DEV_LINEAGE_INSTANCE,
    OPENLINEAGE_NAMESPACE=constants.DEV_OPENLINEAGE_NAMESPACE,
    KEY_PAIR=constants.DEV_KEY_PAIR,
    OPENLINEAGE_SG=s3.OPENLINEAGE_SG,
    env=constants.DEV_ENV,
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
    REDSHIFT_NUM_NODES=constants.DEV_REDSHIFT_NUM_NODES,
    REDSHIFT_NODE_TYPE=constants.DEV_REDSHIFT_NODE_TYPE,
    REDSHIFT_CLUSTER_TYPE=constants.DEV_REDSHIFT_CLUSTER_TYPE,
    REDSHIFT_SG=s3.REDSHIFT_SG,
    env=constants.DEV_ENV,
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
    env=constants.DEV_ENV,
)
mwaa.add_dependency(redshift)

# Production pipeline
# Pipeline(app, f"{constants.CDK_APP_NAME}-pipeline", env=constants.PIPELINE_ENV)

app.synth()
