# import modules
from aws_cdk import (
    core,
    aws_glue as glue,
    aws_iam as iam,
    aws_lakeformation as lf,
)


class LfStack(core.Stack):
    """create the lf admin
    register the s3 buckets
    create the databases
    """

    def __init__(
        self, scope: core.Construct, id: str, vpc_stack, constants: dict, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # add constants from context to output props
        self.output_props = constants.copy()

        # create data lake administrator group
        lf_admin = iam.Role(
            self,
            "lf_admin",
            assumed_by=iam.AccountRootPrincipal(),
        )

        # create the data lake user roles
        lf_engineer = iam.Role(
            self,
            "lf_engineer",
            assumed_by=iam.AccountRootPrincipal(),
        )
        lf_analyst = iam.Role(
            self,
            "lf_analyst",
            assumed_by=iam.AccountRootPrincipal(),
        )
        lf_datascientist = iam.Role(
            self,
            "lf_datascientist",
            assumed_by=iam.AccountRootPrincipal(),
        )

        # create the lakeformation admins
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
        # athena results bucket should not be registered with LF
        lf.CfnResource(
            self,
            "s3_bucket_logs_register",
            resource_arn=vpc_stack.output_props["s3_bucket_logs"].bucket_arn,
            use_service_linked_role=True,
            role_arn=lf_admin.role_arn,
        )

        lf.CfnResource(
            self,
            "s3_bucket_raw_scripts",
            resource_arn=vpc_stack.output_props["s3_bucket_scripts"].bucket_arn,
            use_service_linked_role=True,
            role_arn=lf_admin.role_arn,
        )

        lf.CfnResource(
            self,
            "s3_bucket_raw_register",
            resource_arn=vpc_stack.output_props["s3_bucket_raw"].bucket_arn,
            use_service_linked_role=True,
            role_arn=lf_admin.role_arn,
        )

        lf.CfnResource(
            self,
            "s3_bucket_processed_register",
            resource_arn=vpc_stack.output_props["s3_bucket_processed"].bucket_arn,
            use_service_linked_role=True,
            role_arn=lf_admin.role_arn,
        )

        #lf.CfnResource(
        #    self,
        #    "s3_bucket_serving_register",
        #    resource_arn=vpc_stack.output_props["s3_bucket_serving"].bucket_arn,
        #    use_service_linked_role=True,
        #    role_arn=lf_admin.role_arn,
        #)


        # create the databases
        dl_db_raw = glue.Database(
            self,
            "dl_db_raw",
            database_name="dl_raw",
            location_uri=f"s3://{vpc_stack.output_props['s3_bucket_raw'].bucket_name}",
        )

        dl_db_processed = glue.Database(
            self,
            "dl_db_processed",
            database_name="dl_processed",
            location_uri=f"s3://{vpc_stack.output_props['s3_bucket_processed'].bucket_name}",
        )

        #dl_db_serving = glue.Database(
        #    self,
        #    "dl_db_serving",
        #    database_name="dl_serving",
        #    location_uri=f"s3://{vpc_stack.output_props['s3_bucket_serving'].bucket_name}",
        #)

        self.output_props["dl_db_raw"] = dl_db_raw
        self.output_props["dl_db_processed"] = dl_db_processed
        #self.output_props["dl_db_serving"] = dl_db_serving

    # properties
    @property
    def outputs(self):
        return self.output_props