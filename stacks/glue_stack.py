# import modules
from aws_cdk import core, aws_glue as glue, aws_iam as iam, aws_lakeformation as lf


class GlueStack(core.Stack):
    """ create crawlers for the s3 buckets 
    """

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        vpc_stack,
        lf_stack,
        constants: dict = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # crawler role
        crawler_role = iam.Role(
            self,
            "crawler_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        # lf permissions for the crawler role
        lf.CfnPermissions(
            self,
            "crawler_role_permissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=crawler_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    name=lf_stack.get_glue_database_raw.database_name
                )
            ),
        )

        # the raw bucket crawler
        crawler_raw = glue.CfnCrawler(
            self,
            "crawler_raw",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=vpc_stack.s3_bucket_raw.bucket_name
                    )
                ],
            ),
            database_name=lf_stack.get_glue_database_raw.database_name,
            role=crawler_role.role_name,
        )
        core.Tag.add(crawler_raw, "project", constants["PROJECT_TAG"])
