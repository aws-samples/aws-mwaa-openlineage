#!/usr/bin/env python3

# import modules
import os
from aws_cdk import core

# import cdk classes
from stacks.vpc_stack import VpcStack

app = core.App()

# Vpc stack
vpc_stack = VpcStack(
    app,
    "emr-vpc",
    env=core.Environment(
        account=os.environ["CDK_DEFAULT_ACCOUNT"],
        region=os.environ["CDK_DEFAULT_REGION"],
    ),
)

# synth the app
app.synth()
