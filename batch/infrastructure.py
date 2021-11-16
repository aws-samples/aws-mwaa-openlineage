# import modules
import pathlib
from typing_extensions import runtime
from aws_cdk.aws_logs import RetentionDays
from constructs import Construct
from aws_cdk import (
    Duration,
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_emr as emr,
    aws_events as events,
    aws_events_targets as targets,
    aws_glue as glue,
    aws_iam as iam,
    aws_lakeformation as lf,
    aws_lambda as _lambda,
    aws_s3 as s3,
    Aws,
    RemovalPolicy,
    Stack,
)
from aws_cdk.aws_lambda import Runtime

from pathlib import Path


class Glue(Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        S3_BUCKET_RAW_ARN: str,
        S3_BUCKET_RAW_NAME: str,
        GLUE_DB_PREFIX: str,
        OPENLINEAGE_API: str,
        OPENLINEAGE_NAMESPACE: str,
        VPC: ec2.Vpc,
    ):
        super().__init__(scope, id)

        # to send lineage data to openlineage
        tablelineage_lambda = _lambda.DockerImageFunction(
            self,
            "tablelineage_lambda",
            code=_lambda.DockerImageCode.from_image_asset(
                str(Path(__file__).parent.joinpath("runtime/tablelineage"))
            ),
            description="Send events to openlineage",
            environment={
                "OPENLINEAGE_API": OPENLINEAGE_API,
                "OPENLINEAGE_NAMESPACE": OPENLINEAGE_NAMESPACE,
            },
            log_retention=RetentionDays.ONE_WEEK,
            timeout=Duration.seconds(30),
            vpc=VPC,
        )
        # add lambda permissions
        tablelineage_lambda_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["glue:GetTable", "glue:GetTableVersion"],
            resources=["*"],
        )
        # add the role permissions
        tablelineage_lambda.add_to_role_policy(statement=tablelineage_lambda_policy)

        # event rule to send to tablelineage_lambda
        tableevents = events.Rule(
            self,
            "tablevents",
            description="Glue table updates to openlineage",
            targets=[targets.LambdaFunction(tablelineage_lambda)],
            event_pattern={
                "source": ["aws.glue"],
                "detail_type": ["Glue Data Catalog Table State Change"],
            },
        )

        # glue database for the tables
        glue_db_name = GLUE_DB_PREFIX
        database = glue.CfnDatabase(
            self,
            "database",
            catalog_id=Aws.ACCOUNT_ID,
            database_input={"Name": glue_db_name},
        )
        database.apply_removal_policy(policy=RemovalPolicy.DESTROY)

        # glue crawler role
        crawler_role = iam.Role(
            self,
            "crawler_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            inline_policies=[
                iam.PolicyDocument(
                    statements=[
                        # iam.PolicyStatement(
                        #    effect=iam.Effect.ALLOW,
                        #    actions=["lakeformation:GetDataAccess"],
                        #    resources=["*"],
                        # ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["s3:*"],
                            resources=[
                                S3_BUCKET_RAW_ARN,
                                f"{S3_BUCKET_RAW_ARN}/*",
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

        # permisisons if lf
        # if constants["PERMISSIONS"] == "Lake Formation":
        #     # lf database permissions for the crawler role
        #    lf.CfnPermissions(
        #       self,
        #      "crawler_role_db_permissions",
        #         data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
        #            data_lake_principal_identifier=crawler_role.role_arn
        #        ),
        #        resource=lf.CfnPermissions.ResourceProperty(
        #            database_resource=lf.CfnPermissions.DatabaseResourceProperty(
        #                name=constants["dl_db_raw"].database_name
        #            )
        #        ),
        #        permissions=["ALTER", "CREATE_TABLE", "DROP"],
        #    )
        #
        #            # lf location permissions for the crawler role
        #            lf.CfnPermissions(
        #                self,
        #                "crawler_role_loc_permissions",
        #                data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
        #                    data_lake_principal_identifier=crawler_role.role_arn
        #                ),
        #                resource=lf.CfnPermissions.ResourceProperty(
        #                    data_location_resource=lf.CfnPermissions.DataLocationResourceProperty(
        #                        s3_resource=constants["s3_bucket_raw"].bucket_arn
        #                    )
        #                ),
        #                permissions=["DATA_LOCATION_ACCESS"],
        #            )
        #
        # the raw bucket crawler
        crawler_raw = glue.CfnCrawler(
            self,
            "crawler_raw",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(path=S3_BUCKET_RAW_NAME)],
            ),
            # classifiers=[customer_classifier.csv_classifier.name],
            database_name=glue_db_name,
            role=crawler_role.role_name,
        )

        # create glue job for raw to processed
        # glue_job_processed = glue.CfnJob(
        #    self, "glue_job_processed"  # default_arguments={"--conf": "", "--conf": ""}
        # )


class EMR(Stack):
    """ """

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        VPC: ec2.Vpc,
        EMR_NAME: str = None,
        EMR_RELEASE_LABEL: str = None,
        EMR_CORE_INSTANCE_COUNT: str = None,
        EMR_CORE_INSTANCE_TYPE: str = None,
        EMR_MASTER_INSTANCE_COUNT: str = None,
        EMR_MASTER_INSTANCE_TYPE: str = None,
    ):
        super().__init__(scope, id)

        # create emr bucket
        s3_bucket_emr = s3.Bucket(
            self,
            "s3_bucket_emr",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # emr job flow role
        emr_jobflow_role = iam.Role(
            self,
            "emr_jobflow_role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonElasticMapReduceforEC2Role"
                )
            ],
        )

        # emr service role
        emr_service_role = iam.Role(
            self,
            "emr_service_role",
            assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonElasticMapReduceRole"
                )
            ],
            # inline_policies=[read_scripts_document],
        )

        # emr job flow profile
        emr_jobflow_profile = iam.CfnInstanceProfile(
            self,
            "emr_jobflow_profile",
            roles=[emr_jobflow_role.role_name],
            instance_profile_name=emr_jobflow_role.role_name,
        )

        emr_cluster = emr.CfnCluster(
            self,
            "emr_cluster",
            instances=emr.CfnCluster.JobFlowInstancesConfigProperty(
                core_instance_fleet=emr.CfnCluster.InstanceFleetConfigProperty(
                    instance_type_configs=[
                        emr.CfnCluster.InstanceTypeConfigProperty(
                            instance_type=EMR_CORE_INSTANCE_TYPE
                        )
                    ],
                    target_spot_capacity=EMR_CORE_INSTANCE_COUNT,
                ),
                ec2_subnet_ids=VPC.select_subnets(
                    subnet_type=ec2.SubnetType.PRIVATE
                ).subnet_ids,
                hadoop_version="Amazon",
                keep_job_flow_alive_when_no_steps=False,
                master_instance_fleet=emr.CfnCluster.InstanceFleetConfigProperty(
                    instance_type_configs=[
                        emr.CfnCluster.InstanceTypeConfigProperty(
                            instance_type=EMR_MASTER_INSTANCE_TYPE
                        )
                    ],
                    target_spot_capacity=EMR_MASTER_INSTANCE_COUNT,
                ),
            ),
            job_flow_role=emr_jobflow_profile.instance_profile_name,
            name=EMR_NAME,
            service_role=emr_service_role.role_name,
            applications=[emr.CfnCluster.ApplicationProperty(name="Spark")],
            configurations=[
                # use python3 for pyspark
                emr.CfnCluster.ConfigurationProperty(
                    classification="spark-env",
                    configurations=[
                        emr.CfnCluster.ConfigurationProperty(
                            classification="export",
                            configuration_properties={
                                "PYSPARK_PYTHON": "/usr/bin/python3",
                                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
                            },
                        )
                    ],
                ),
                # enable apache arrow
                emr.CfnCluster.ConfigurationProperty(
                    classification="spark-defaults",
                    configuration_properties={
                        "spark.sql.execution.arrow.enabled": "true"
                    },
                ),
                # dedicate cluster to single jobs
                emr.CfnCluster.ConfigurationProperty(
                    classification="spark",
                    configuration_properties={"maximizeResourceAllocation": "true"},
                ),
            ],
            log_uri=f"s3://{s3_bucket_emr.bucket_name}/{Aws.REGION}/elasticmapreduce/",
            release_label=EMR_RELEASE_LABEL,
            visible_to_all_users=True,
            # the job to be done
            # steps=[
            #    emr.CfnCluster.StepConfigProperty(
            #        hadoop_jar_step=emr.CfnCluster.HadoopJarStepConfigProperty(
            #            jar="command-runner.jar",
            #            args=[
            #                "spark-submit",
            #                "--deploy-mode",
            #                "cluster",
            #                f"s3://{vpc_stack.s3_bucket_scripts.bucket_name}/scripts/{pyspark_script}",
            #                "--s3_bucket_data",
            #                f"{vpc_stack.get_s3_bucket_data.bucket_name}",
            #            ],
            #        ),
            #        name=f"{job_name}",
            #        action_on_failure="CONTINUE",
            #    ),
            # ],
        )
        # dependencies
        # self.emr_cluster.node.add_dependency(s3_scripts)
