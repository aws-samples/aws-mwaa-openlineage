# The continuous deployment pipeline
from typing import Any
from aws_cdk import core as cdk
from aws_cdk import pipelines

import constants
from deployment import AWSCDKDataLake 

class Pipeline(cdk.Stack):
    def __init__(self, scope: cdk.Construct, id_: str, **kwargs: Any):
        super().__init__(scope, id_, **kwargs)
        ...
        codepipeline = pipelines.CodePipeline(...)
        self._add_prod_stage(codepipeline)
    ...
    def _add_prod_stage(self, codepipeline: pipelines.CodePipeline) -> None:
        prod_stage = AWSCDKDataLake(
            self,
            f"{constants.CDK_APP_NAME}-prod",
            env=constants.PROD_ENV,
        )
        codepipeline.add_stage(prod_stage, post=[smoke_test_shell_step])