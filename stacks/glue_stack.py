# import modules
from aws_cdk import (
    core,
    aws_glue as glue,
)

class GlueStack(core.Stack):
    """ create crawlers for the s3 buckets 
    """

    def __init__(self, scope: core.Construct, id: str, vpc_stack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
