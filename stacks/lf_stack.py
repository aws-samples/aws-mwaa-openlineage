# import modules
from aws_cdk import (
    core,
    aws_ec2 as ec2,
    aws_glue as glue,
    aws_iam as iam,
    aws_lakeformation as lf,
    aws_s3 as s3,
)
from scripts.custom_resource import CustomResource
from scripts.constants import constants
from pathlib import Path

# set path
dirname = Path(__file__).parent


class LfStack(core.Stack):
    """ create the lf admin
        register the s3 buckets
        create the databases
    """

    def __init__(self, scope: core.Construct, id: str, vpc_stack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # create data lake administrator group
        lf_admin = iam.Role(self, "lf_admin", assumed_by=iam.AccountRootPrincipal(),)

        # create the lakeformation
        lf.CfnDataLakeSettings(
            self,
            "lf_admins",
            admins=[
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=lf_admin.role_arn
                ),
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=f"arn:aws:iam::{core.Aws.ACCOUNT_ID}:user/mcgregf-dev1"
                ),
            ],
        )

        # register the s3 buckets
        lf.CfnResource(
            self,
            "s3_bucket_raw_scripts",
            resource_arn=vpc_stack.get_s3_bucket_scripts.bucket_arn,
            use_service_linked_role=True,
            role_arn=lf_admin.role_arn,
        )

        lf.CfnResource(
            self,
            "s3_bucket_raw_register",
            resource_arn=vpc_stack.get_s3_bucket_raw.bucket_arn,
            use_service_linked_role=True,
            role_arn=lf_admin.role_arn,
        )

        lf.CfnResource(
            self,
            "s3_bucket_processed_register",
            resource_arn=vpc_stack.get_s3_bucket_processed.bucket_arn,
            use_service_linked_role=True,
            role_arn=lf_admin.role_arn,
        )

        lf.CfnResource(
            self,
            "s3_bucket_serving_register",
            resource_arn=vpc_stack.get_s3_bucket_serving.bucket_arn,
            use_service_linked_role=True,
            role_arn=lf_admin.role_arn,
        )

        lf.CfnResource(
            self,
            "s3_bucket_logs_register",
            resource_arn=vpc_stack.get_s3_bucket_logs.bucket_arn,
            use_service_linked_role=True,
            role_arn=lf_admin.role_arn,
        )

        # create the databases
        dl_db_raw = glue.Database(
            self,
            "dl_db_raw",
            database_name="dl_raw",
            location_uri=f"s3://{vpc_stack.get_s3_bucket_raw.bucket_name}",
        )
        dl_db_processed = glue.Database(
            self,
            "dl_db_processed",
            database_name="dl_processed",
            location_uri=f"s3://{vpc_stack.get_s3_bucket_processed.bucket_name}",
        )
        dl_db_serving = glue.Database(
            self,
            "dl_db_serving",
            database_name="dl_serving",
            location_uri=f"s3://{vpc_stack.get_s3_bucket_serving.bucket_name}",
        )
