# import modules
from constructs import Construct
from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3 as s3,
    Aws,
    CfnOutput,
    RemovalPolicy,
    Stack,
    aws_iam as iam,
    aws_glue_alpha as glue,
    aws_secretsmanager as sm,
)
import aws_cdk as cdk

from pathlib import Path
dirname = Path(__file__).parent

class Glue(Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        S3_BUCKET_CURATED: s3.Bucket,
        GLUE_RAW_DB: str,
        GLUE_CURATED_DB: str,
        REDSHIFT_DB_NAME: str,
        REDSHIFT_WORKGROUP: str,
        REDSHIFT_MASTER_USERNAME: str,
        VPC: ec2.Vpc,
        GLUE_SG: ec2.SecurityGroup,
        OPENLINEAGE_API: str,
        REDSHIFT_IAM_ROLE: iam.Role,
        REDSHIFT_SECRET: sm.Secret,
        **kwargs
    ):
        super().__init__(scope, id, **kwargs)
        
        glue_job_role: iam.IRole = iam.Role(
            self,
            "glue_job_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("SecretsManagerReadWrite"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftAllCommandsFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftFullAccess")
            ],
        )  # type: ignore

        glue_vpc_connection = glue.Connection(
            self,
            "glueVPCConnection",
            type=glue.ConnectionType.NETWORK,
            connection_name="Glue VPC Connection",
            description="Connection to force Glue jobs to run in the VPC.",
            security_groups=[GLUE_SG],
            subnet=VPC.private_subnets[0]
        )

        glue_temp_bucket = s3.Bucket(
            self,
            "glue",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(enabled=True, expiration=cdk.Duration.days(7))
            ]
        )

        glue.Job(
            self,
            "glueJob",
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V4_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset("transform/runtime/scripts/raw_to_transformed.py"),
            ),
            connections=[glue_vpc_connection],
            default_arguments={
                "--TempDir": f"s3://{glue_temp_bucket.bucket_name}/",
                "--table_name": "default",
                "--s3_target_bucket": S3_BUCKET_CURATED.bucket_name,
                "--raw_db": GLUE_RAW_DB,
                "--curated_db": GLUE_CURATED_DB,
            },
            role=glue_job_role,
            worker_count=4,
            worker_type=glue.WorkerType.G_1_X,
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            enable_profiling_metrics=True
        )
