# import modules
from aws_cdk import core, aws_glue as glue, aws_iam as iam, aws_lakeformation as lf


class GlueStack(core.Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        constants: dict = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # glue crawler role
        crawler_role = iam.Role(
            self,
            "crawler_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            inline_policies=[
                iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["lakeformation:GetDataAccess"],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["s3:*"],
                            resources=[
                                constants["s3_bucket_raw"].bucket_arn,
                                f"{constants['s3_bucket_raw'].bucket_arn}/*",
                            ],
                        ),
                    ]
                ),
            ],
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

        # lf database permissions for the crawler role
        lf.CfnPermissions(
            self,
            "crawler_role_db_permissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=crawler_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    name=constants["dl_db_raw"].database_name
                )
            ),
            permissions=["ALTER", "CREATE_TABLE", "DROP"],
        )

        # lf location permissions for the crawler role
        lf.CfnPermissions(
            self,
            "crawler_role_loc_permissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=crawler_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                data_location_resource=lf.CfnPermissions.DataLocationResourceProperty(
                    s3_resource=constants["s3_bucket_raw"].bucket_arn
                )
            ),
            permissions=["DATA_LOCATION_ACCESS"],
        )

        # the raw bucket crawler
        crawler_raw = glue.CfnCrawler(
            self,
            "crawler_raw",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=constants["s3_bucket_raw"].bucket_name
                    )
                ],
            ),
            # classifiers=[customer_classifier.csv_classifier.name],
            database_name=constants["dl_db_raw"].database_name,
            role=crawler_role.role_name,
        )
        core.Tags.of(crawler_raw).add("project", constants["PROJECT_TAG"])
        
        # create glue job for raw to processed
        #glue_job_processed = glue.CfnJob(self, "glue_job_processed", command=(scriptLocation=""))

        # output props
        self.output_props = {}

    # properties
    @property
    def outputs(self):
        return self.output_props