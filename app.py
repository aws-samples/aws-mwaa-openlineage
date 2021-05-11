#!/usr/bin/env python3

# import modules
import os
from aws_cdk import core

# import cdk classes
from stacks.vpc_stack import VpcStack
from stacks.mwaa_stack import MwaaStack
from stacks.lf_stack import LfStack
from stacks.glue_stack import GlueStack

app = core.App()

this_env = core.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"],
)

constants = app.node.try_get_context("constants")

# Vpc stack
vpc_stack = VpcStack(app, "dl-vpc", constants=constants, env=this_env)
constants.update(vpc_stack.output_props)

# mwaa stack
mwaa_stack = MwaaStack(app, "dl-mwaa", constants=constants, env=this_env)
constants.update(mwaa_stack.output_props)


# Lf stack
lf_stack = LfStack(app, "dl-lf", constants=constants, env=this_env)
constants.update(lf_stack.output_props)

# Glue stack
glue_stack = GlueStack(app, "dl-glue", constants=constants, env=this_env)
constants.update(glue_stack.output_props)

# synth the app
app.synth()
