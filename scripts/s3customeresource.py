import os
from aws_cdk import (
    aws_cloudformation as cfn,
    aws_iam as iam,
    aws_lambda as lambda_,
    core,
    aws_logs as logs,
)
from pathlib import Path

# set path
dirname = Path(__file__).parent


class CustomResource(core.Construct):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id)

        # custom resource
        resource = cfn.CustomResource(
            self,
            "Resource",
            provider=cfn.CustomResourceProvider.lambda_(
                lambda_.SingletonFunction(
                    self,
                    "Singleton",
                    description=f"To delete files from s3 bucket",
                    uuid="f7d4f730-4ee1-11e8-9c2d-fa7ae01bbebc",
                    code=lambda_.Code.from_asset(
                        str(dirname.parent.joinpath("scripts/lambda"))
                    ),
                    handler="s3bucketcleaner.lambda_handler",
                    timeout=core.Duration.seconds(300),
                    runtime=lambda_.Runtime.PYTHON_3_8,
                    initial_policy=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["s3:ListBucket"],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:DeleteObject",
                            ],
                            resources=["*"],
                        ),
                    ],
                    log_retention=logs.RetentionDays.ONE_DAY,
                )
            ),
            properties=kwargs,
        )
        # response
        self.response = resource.get_att("Response")
