# import modules
from aws_cdk import (
    core,
    aws_events as eb,
    aws_events_targets as et,
    aws_lambda as lambda_,
)
from pathlib import Path

# set path
dirname = Path(__file__).parent


class EventStack(core.Stack):
    """ create crawlers for the s3 buckets 
    """

    def __init__(
        self, scope: core.Construct, id: str, constants: dict, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # create parameters lambda function
        with open(
            str(dirname.parent.joinpath("scripts/dg_events.py")), encoding="utf-8"
        ) as fp:
            set_parameters_code_body = fp.read()

        # set parameters lambda
        dg_lambda = lambda_.Function(
            self,
            "dg_lambda",
            code=lambda_.InlineCode(set_parameters_code_body),
            handler="index.main",
            runtime=lambda_.Runtime.PYTHON_3_7,
            # initial_policy=get_jobflowid_policy,
        )
        core.Tag.add(dg_lambda, "project", constants["PROJECT_TAG"])

        # create the event bus
        dg_eventbus = eb.EventBus(self, "dg_eventbus")

        # rule to capture glue table changes
        dg_glue_table_update = eb.Rule(
            self,
            "dg_glue_table_update",
            event_bus=dg_eventbus,
            event_pattern=eb.EventPattern(source=["aws.glue"]),
            targets=[et.LambdaFunction(dg_lambda)],
        )

