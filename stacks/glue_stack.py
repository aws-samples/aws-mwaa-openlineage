# import modules
from aws_cdk import core, aws_glue as glue, aws_iam as iam, aws_lakeformation as lf


class GlueStack(core.Stack):
    """create crawlers for the s3 buckets"""

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

        # add constants from context to output props
        self.output_props = constants.copy()

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
                                vpc_stack.output_props["s3_bucket_raw"].bucket_arn,
                                f"{vpc_stack.output_props['s3_bucket_raw'].bucket_arn}/*",
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
                    name=lf_stack.output_props["dl_db_raw"].database_name
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
                    s3_resource=vpc_stack.output_props["s3_bucket_raw"].bucket_arn
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
                        path=vpc_stack.output_props["s3_bucket_raw"].bucket_name
                    )
                ],
            ),
            # classifiers=[customer_classifier.csv_classifier.name],
            database_name=lf_stack.output_props["dl_db_raw"].database_name,
            role=crawler_role.role_name,
        )
        core.Tags.of(crawler_raw).add("project", constants["PROJECT_TAG"])

        # create processed table
        firehose_processed_table = glue.Table(
            self,
            "firehose_processed_table",
            columns=[glue.Column(name="tempcolumn", type=glue.Schema.DOUBLE)],
            database=lf_stack.output_props["dl_db_processed"],
            bucket=vpc_stack.output_props["s3_bucket_processed"],
            s3_prefix="firehose_processed/",
        )

    # properties
    @property
    def outputs(self):
        return self.output_props