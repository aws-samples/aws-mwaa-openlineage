# import modules
from constructs import Construct
from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_emrserverless as emrs,
    aws_emr as emr,
    aws_s3_deployment as s3_deploy,
    Aws,
    Tags,
    CfnOutput,
    RemovalPolicy,
    Stack,
    aws_iam as iam,
    aws_secretsmanager as sm,
)
import aws_cdk as cdk

from pathlib import Path
dirname = Path(__file__).parent

class EMR(Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        VPC: ec2.Vpc,
        EMR_SG: ec2.SecurityGroup,
        **kwargs
    ):
        super().__init__(scope, id, **kwargs)
        
        # create s3 bucket for emr
        s3_bucket_emr = s3.Bucket(
            self,
            "s3_bucket_emr",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
            enforce_ssl=True,
        )
        # tag the bucket
        Tags.of(s3_bucket_emr).add("purpose", "EMR")
        
        emrs_role = iam.Role(
            self,
            "emrs_role",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "SecretsManagerReadWrite"
                )
            ]
        )
        
        emrs_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:DeleteObject",
                    "glue:GetDatabase",
                    "glue:CreateDatabase",
                    "glue:GetDataBases",
                    "glue:CreateTable",
                    "glue:GetTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:CreatePartition",
                    "glue:BatchCreatePartition",
                    "glue:GetUserDefinedFunctions"
                ],
                resources=["*"],
            )
        )
        
         # Create an EMR Studio
        emr_studio = emr.CfnStudio(
            self,
            "EMRStudio",
            vpc_id = VPC.vpc_id,
            engine_security_group_id=EMR_SG.security_group_id,
            subnet_ids=VPC.select_subnets(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ).subnet_ids,
            auth_mode="IAM",
            default_s3_location=f"s3://{s3_bucket_emr.bucket_name}/emr-default",
            name="MyEMRStudio",
            service_role=iam.Role(
                self,
                "EMRStudioServiceRole",
                assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonEMRFullAccessPolicy_v2"
                    ),
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonS3FullAccess"
                    )
                ],
            ).role_arn,
            workspace_security_group_id=EMR_SG.security_group_id,
        )

        
        
        emrs_application = emrs.CfnApplication(
            self, 
            "spark_app",
            release_label="emr-7.1.0",
            type="SPARK",

            # the properties below are optional
            name="spark-3.5",
            auto_stop_configuration=emrs.CfnApplication.AutoStopConfigurationProperty(
                enabled=True, idle_timeout_minutes=100
            ),
            initial_capacity=[
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Driver",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=2,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Executor",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=2,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
            ],
            
            network_configuration=emrs.CfnApplication.NetworkConfigurationProperty(
                security_group_ids=[EMR_SG.security_group_id],
                subnet_ids=VPC.select_subnets(
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
                ).subnet_ids
            ),
            runtime_configuration=[emrs.CfnApplication.ConfigurationObjectProperty(
                classification="spark-defaults",
                properties={
                    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
                    "spark.jars.packages": "io.openlineage:openlineage-spark:1.9.1",
                    "spark.openlineage.namespace": "spark",
                    "spark.openlineage.transport.type": "console",
                }
            ),
            emrs.CfnApplication.ConfigurationObjectProperty(
                classification="hive-site",
                properties={
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            )]
        )
        
        # share security groups
        self.EMRS_ROLE_ARN = emrs_role.role_arn
        self.EMRS_APPLICATION_ID = emrs_application.ref

        CfnOutput(
            self,
            "EMR Serverless IAM Role ARN",
            value=self.EMRS_ROLE_ARN,
            export_name="emrs-role-arn",
        )
        
        CfnOutput(
            self,
            "EMR Serverless Application ID",
            value=self.EMRS_APPLICATION_ID,
            export_name="emrs-application-id",
        )
        
        # Output the EMR Studio URL
        CfnOutput(
            self,
            "EMRStudioURL",
            value=f"{emr_studio.attr_url}",
            description="EMR Studio URL",
        )