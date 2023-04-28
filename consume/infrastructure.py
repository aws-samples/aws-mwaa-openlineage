# import modules
from constructs import Construct
from aws_cdk import (
    aws_athena as athena,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_s3 as s3,
    aws_redshift as redshift,
    aws_redshiftserverless as rs,
    aws_secretsmanager as sm,
    Aws,
    CfnOutput,
    RemovalPolicy,
    Stack,
    SecretValue,
    aws_glue_alpha as glue
)
import aws_cdk as cdk


class Redshift(Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        S3_BUCKET_RAW: s3.Bucket,
        S3_BUCKET_CURATED: s3.Bucket,
        REDSHIFT_DB_NAME: str,
        REDSHIFT_NAMESPACE: str,
        REDSHIFT_WORKGROUP: str,
        REDSHIFT_MASTER_USERNAME: str,
        REDSHIFT_SG: ec2.SecurityGroup,
        VPC: ec2.Vpc,
        GLUE_SG: ec2.SecurityGroup,
        LINEAGE_API_URL: str,
        **kwargs
    ):
        super().__init__(scope, id, **kwargs)

        redshift_password = sm.Secret(
            self,
            "redshift_password",
            description="Redshift password",
            secret_name="REDSHIFT_PASSWORD",
            generate_secret_string=sm.SecretStringGenerator(exclude_punctuation=True),
            removal_policy=RemovalPolicy.DESTROY,
        )

        redshift_cluster_role = iam.Role(
            self,
            "redshift_cluster_role",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftAllCommandsFullAccess")
            ],
        )

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

        S3_BUCKET_RAW.grant_read_write(redshift_cluster_role)

        rs_namespace = rs.CfnNamespace(
            self,
            "redshiftServerlessNamespace",
            namespace_name=REDSHIFT_NAMESPACE,
            db_name=REDSHIFT_DB_NAME,
            default_iam_role_arn=redshift_cluster_role.role_arn,
            iam_roles=[redshift_cluster_role.role_arn],
            admin_username=REDSHIFT_MASTER_USERNAME,
            admin_user_password=redshift_password.secret_value.unsafe_unwrap(),
        )

        rs_workgroup = rs.CfnWorkgroup(
            self,
            "redshiftServerlessWorkgroup",
            workgroup_name=REDSHIFT_WORKGROUP,
            base_capacity=32,
            namespace_name=rs_namespace.ref,
            security_group_ids=[REDSHIFT_SG.security_group_id],
            subnet_ids=VPC.select_subnets(
                subnet_type=ec2.SubnetType.PUBLIC
            ).subnet_ids,
        )

        secret_redshift_conn = sm.Secret(
            self,
            "secret_redshift_conn",
            description="Connector for Redshift",
            secret_name="airflow/connections/REDSHIFT_CONNECTOR",
            secret_string_value=SecretValue.unsafe_plain_text(
                f"postgres://{REDSHIFT_MASTER_USERNAME}:{redshift_password.secret_value.to_string()}"
                f"@{REDSHIFT_WORKGROUP}.{Aws.ACCOUNT_ID}.{Aws.REGION}.redshift-serverless.amazonaws.com:5439"
                f"/{rs_namespace.db_name}"
                ),
            removal_policy=RemovalPolicy.DESTROY,
        )

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
            "glue-temp-dir",
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
                script=glue.Code.from_asset("consume/runtime/glue/example_job/example_job.py"),
                extra_jars=[glue.Code.from_asset("consume/runtime/glue/jars/openlineage-spark-0.22.0.jar")],
                extra_jars_first=True
            ),
            connections=[glue_vpc_connection],
            default_arguments={
                "--conf": "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener"
                + f" --conf spark.openlineage.transport.url={LINEAGE_API_URL}"
                + " --conf spark.openlineage.transport.type=http",
                "--TempDir": f"s3://{glue_temp_bucket.bucket_name}/",
                "--redshift_conn_string":
                    f"jdbc:redshift://{REDSHIFT_WORKGROUP}.{Aws.ACCOUNT_ID}.{Aws.REGION}"
                    + f".redshift-serverless.amazonaws.com:5439/{rs_namespace.db_name}",
                "--redshift_user": REDSHIFT_MASTER_USERNAME,
                "--redshift_password_secret_id": redshift_password.secret_arn,
                "--output_bucket": S3_BUCKET_CURATED.bucket_name,
                "--redshift_cluster_role": redshift_cluster_role.role_arn,
                "--lineage_api_url": LINEAGE_API_URL,
                "--additional-python-modules": "openlineage-python"
            },
            role=glue_job_role,
            worker_count=4,
            worker_type=glue.WorkerType.G_1_X,
            continuous_logging=glue.ContinuousLoggingProps(enabled=True),
            enable_profiling_metrics=True
        )


        output_1 = CfnOutput(
            self,
            "RedshiftServerlessEndpoint",
            value=f"{REDSHIFT_WORKGROUP}.{Aws.ACCOUNT_ID}.{Aws.REGION}.redshift-serverless.amazonaws.com",
            description=f"RedshiftServerlessEndpoint",
        )
        output_2 = CfnOutput(
            self,
            "RedshiftMasterUser",
            value=REDSHIFT_MASTER_USERNAME,
            description=f"Redshift master username",
        )

        output_3 = CfnOutput(
            self,
            "RedshiftClusterPassword",
            value=(
                f"https://console.aws.amazon.com/secretsmanager/home?region="
                f"{Aws.REGION}"
                f"#/secret?name="
                f"{redshift_password.secret_arn}"
            ),
            description=f"Redshift Cluster Password in Secrets Manager",
        )
        
        output_4 = CfnOutput(
            self,
            "RedshiftConnectionString",
            value=(
                f"postgres://{REDSHIFT_MASTER_USERNAME}:<Password>"
                f"@{REDSHIFT_WORKGROUP}.{Aws.ACCOUNT_ID}.{Aws.REGION}.redshift-serverless.amazonaws.com:5439"
                f"/{rs_namespace.db_name}"
            ),
            description=f"Redshift Connection String",
        )
