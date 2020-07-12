#!/usr/bin/env python3

# import modules
import os
from aws_cdk import core

# import cdk classes
from stacks.vpc_stack import VpcStack
from stacks.lf_stack import LfStack
from stacks.event_stack import EventStack

app = core.App()

this_env = core.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"],
)

constants = app.node.try_get_context("constants")

# Vpc stack
vpc_stack = VpcStack(app, "dl-vpc", constants=constants, env=this_env)

# Lf stack
lf_stack = LfStack(app, "dl-lf", vpc_stack, env=this_env)

# Event stack
event_stack = EventStack(app, "dl-event", constants=constants, env=this_env)

# synth the app
app.synth()
