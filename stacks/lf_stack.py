# import modules
from aws_cdk import (
    core,
    aws_ec2 as ec2,
    aws_lakeformation as lf,
    aws_s3 as s3,
    aws_iam as iam,
)
from scripts.custom_resource import CustomResource
from scripts.constants import constants
from pathlib import Path

# set path
dirname = Path(__file__).parent


class LfStack(core.Stack):
    """ create lake formation
        create the lf admin
        register the s3 buckets
    """

    def __init__(self, scope: core.Construct, id: str, vpc_stack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # create the lakeformation
        xyz = lf.CfnDataLakeSettings(
            self, "xyz", admins=[lf.CfnDataLakeSettings.DataLakePrincipalProperty()]
        )
