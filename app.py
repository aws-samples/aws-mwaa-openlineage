#!/usr/bin/env python3
from aws_cdk import App, Stack
from aws_cdk import pipelines

# The AWS CDK application entry point
import constants
from deployment import CDKDataLake

app = App()

# Development
CDKDataLake(
    app,
    f"{constants.CDK_APP_NAME}-dev",
    env=constants.DEV_ENV,
)

# Production pipeline
# Pipeline(app, f"{constants.CDK_APP_NAME}-pipeline", env=constants.PIPELINE_ENV)

app.synth()
