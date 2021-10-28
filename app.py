#!/usr/bin/env python3
from aws_cdk import core as cdk

# The AWS CDK application entry point
import constants
from deployment import CDKDataLake
from pipeline import Pipeline

app = cdk.App()

# Development
CDKDataLake(
    app,
    f"{constants.CDK_APP_NAME}-dev",
    env=constants.DEV_ENV,
)

# Production pipeline
# Pipeline(app, f"{constants.CDK_APP_NAME}-pipeline", env=constants.PIPELINE_ENV)

app.synth()
