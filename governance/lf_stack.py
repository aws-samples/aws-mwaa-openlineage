# import modules
from aws_cdk import (
    core,
    aws_ec2 as ec2,
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
        self, scope: core.Construct, id: str, constants: dict, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        if constants["PERMISSIONS"] == 'Lake Formation':
            print("Using Lake Formation permissions")

            # create the vpc endpoint for lake formation
            constants["vpc"].add_interface_endpoint(
                "lakeformation_endpoint",
                service=ec2.InterfaceVpcEndpointAwsService(name="lakeformation"),
            )

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
                        data_lake_principal_identifier=f"arn:aws:iam::{core.Aws.ACCOUNT_ID}:user/{constants['lf_admin_user']}"
                    ),
                ],
            )

            # register the s3 buckets
            # athena results bucket should not be registered with LF
            lf.CfnResource(
                self,
                "s3_bucket_logs_register",
                resource_arn=constants["s3_bucket_logs"].bucket_arn,
                use_service_linked_role=True,
                role_arn=lf_admin.role_arn,
            )

            lf.CfnResource(
                self,
                "s3_bucket_raw_scripts",
                resource_arn=constants["s3_bucket_scripts"].bucket_arn,
                use_service_linked_role=True,
                role_arn=lf_admin.role_arn,
            )

            lf.CfnResource(
                self,
                "s3_bucket_raw_register",
                resource_arn=constants["s3_bucket_raw"].bucket_arn,
                use_service_linked_role=True,
                role_arn=lf_admin.role_arn,
            )

            lf.CfnResource(
                self,
                "s3_bucket_stage_register",
                resource_arn=constants["s3_bucket_stage"].bucket_arn,
                use_service_linked_role=True,
                role_arn=lf_admin.role_arn,
            )

            lf.CfnResource(
                self,
                "s3_bucket_serving_register",
                resource_arn=constants["s3_bucket_serving"].bucket_arn,
                use_service_linked_role=True,
                role_arn=lf_admin.role_arn,
            )

            # create the databases
            dl_db_raw = glue.Database(
                self,
                "dl_db_raw",
                database_name="dl_raw",
                location_uri=f"s3://{constants['s3_bucket_raw'].bucket_name}",
            )

            dl_db_stage = glue.Database(
                self,
                "dl_db_stage",
                database_name="dl_stage",
                location_uri=f"s3://{constants['s3_bucket_stage'].bucket_name}",
            )

            dl_db_serving = glue.Database(
                self,
                "dl_db_serving",
                database_name="dl_serving",
                location_uri=f"s3://{constants['s3_bucket_serving'].bucket_name}",
            )

            self.output_props = {}
            self.output_props["dl_db_raw"] = dl_db_raw
            self.output_props["dl_db_stage"] = dl_db_stage
            self.output_props["dl_db_serving"] = dl_db_serving

        else:
            print("Using IAM permissions")

    # properties
    @property
    def outputs(self):
        return self.output_props
