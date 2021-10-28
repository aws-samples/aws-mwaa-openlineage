#!/usr/bin/env python3

# import modules
import os
from aws_cdk import core

# import cdk classes
from stacks.vpc_stack import VpcStack
from stacks.marquez_stack import MarquezStack
from stacks.mwaa_stack import MwaaStack
# from stacks.lf_stack import LfStack
# from stacks.glue_stack import GlueStack

app = core.App()

this_env = core.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"],
)

constants = app.node.try_get_context("constants")

# Vpc stack
vpc_stack = VpcStack(app, "dl-vpc", constants=constants, env=this_env)
constants.update(vpc_stack.output_props)

# marquez stack for lineage
marquez_stack = MarquezStack(app, "dl-marquez", constants=constants, env=this_env)
constants.update(marquez_stack.output_props)

# mwaa stack
mwaa_stack = MwaaStack(app, "dl-mwaa", constants=constants, env=this_env)
constants.update(mwaa_stack.output_props)

# Lf stack
# lf_stack = LfStack(app, "dl-lf", constants=constants, env=this_env)
# constants.update(lf_stack.output_props)

# Glue stack
# glue_stack = GlueStack(app, "dl-glue", constants=constants, env=this_env)
# constants.update(glue_stack.output_props)

print(constants)

# synth the app
app.synth()

# The AWS CDK application entry point
import constants
from deployment import UserManagementBackend
from pipeline import Pipeline

app = cdk.App()

# Development
UserManagementBackend(
    app,
    f"{constants.CDK_APP_NAME}-Dev",
    env=constants.DEV_ENV,
    api_lambda_reserved_concurrency=constants.DEV_API_LAMBDA_RESERVED_CONCURRENCY,
    database_dynamodb_billing_mode=constants.DEV_DATABASE_DYNAMODB_BILLING_MODE,
)

# Production pipeline
Pipeline(app, f"{constants.CDK_APP_NAME}-Pipeline", env=constants.PIPELINE_ENV)

app.synth()