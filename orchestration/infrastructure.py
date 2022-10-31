# import modules
from charset_normalizer import from_path
from constructs import Construct
from aws_cdk import (
    aws_codecommit as codecommit,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_iam as iam,
    aws_logs as logs,
    aws_mwaa as mwaa,
    aws_secretsmanager as _sm,
    Aws,
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
)
from pathlib import Path

# set path
dirname = Path(__file__).parent


class MWAA(Stack):
    """
    create s3 buckets for mwaa
    create mwaa env
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        MWAA_ENV_NAME: str,
        MWAA_ENV_CLASS: str,
        MWAA_REPO_DAG_NAME: str,
        VPC: ec2.Vpc,
        AIRFLOW_SG: ec2.SecurityGroup,
        MWAA_DEPLOY_FILES: bool = False,
        **kwargs
    ):
        super().__init__(scope, id, **kwargs)

        # create s3 bucket for mwaa
        s3_bucket_mwaa = s3.Bucket(
            self,
            "s3_bucket_mwaa",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
            enforce_ssl=True,
        )
        # tag the bucket
        Tags.of(s3_bucket_mwaa).add("purpose", "MWAA")

        # deploy files to mwaa bucket
        if MWAA_DEPLOY_FILES:
            airflow_files = s3_deploy.BucketDeployment(
                self,
                "deploy_requirements",
                destination_bucket=s3_bucket_mwaa,
                sources=[
                    s3_deploy.Source.asset("./orchestration/runtime/mwaa/"),
                ],
                include=["requirements.txt", "plugins.zip"],
                exclude=["requirements.in", "dags/*", "dags.zip", "plugins/*"],
            )

        # create vpc endpoints for mwaa
        mwwa_api_endpoint = VPC.add_interface_endpoint(
            "mwaa_api_endpoint",
            service=ec2.InterfaceVpcEndpointAwsService(name="airflow.api"),
        )
        VPC.add_interface_endpoint(
            "mwaa_env_endpoint",
            service=ec2.InterfaceVpcEndpointAwsService(name="airflow.env"),
        )
        VPC.add_interface_endpoint(
            "mwaa_ops_endpoint",
            service=ec2.InterfaceVpcEndpointAwsService(name="airflow.ops"),
        )

        # role for mwaa
        airflow_role = iam.Role(
            self,
            "airflow_role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
                iam.ServicePrincipal("airflow.amazonaws.com"),
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "SecretsManagerReadWrite"
                )
            ],
        )
        
        airflow_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["airflow:PublishMetrics"],
                resources=["*"],
            )
        )
        
        airflow_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.DENY,
                actions=["s3:ListAllMyBuckets"],
                resources=[
                    s3_bucket_mwaa.bucket_arn,
                    f"{s3_bucket_mwaa.bucket_arn}/*",
                ]
            )
        )
        
        airflow_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject*",
                    "s3:GetBucket*",
                    "s3:List*",
                ],
                resources=[
                    s3_bucket_mwaa.bucket_arn,
                    f"{s3_bucket_mwaa.bucket_arn}/*",
                ],
            )
        )
        
        airflow_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogStream",
                    "logs:CreateLogGroup",
                    "logs:PutLogEvents",
                    "logs:GetLogEvents",
                    "logs:GetLogRecord",
                    "logs:GetLogGroupFields",
                    "logs:GetQueryResults",
                    "logs:DescribeLogGroups",
                ],
                resources=["*"],
            )
        )
        
        airflow_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )
        
        airflow_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "sqs:ChangeMessageVisibility",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl",
                    "sqs:ReceiveMessage",
                    "sqs:SendMessage",
                ],
                resources=[f"arn:aws:sqs:{Aws.REGION}:*:airflow-celery-*"],
            )
        )
            
        airflow_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey*",
                    "kms:Encrypt",
                ],
                not_resources=[f"arn:aws:kms:*:{Aws.ACCOUNT_ID}:key/*"],
                conditions={
                    "StringLike": {
                        "kms:ViaService": [
                            f"sqs.{Aws.REGION}.amazonaws.com"
                        ]
                    }
                },
            )
        )

        
        airflow_env = mwaa.CfnEnvironment(
            self,
            "airflow_env",
            name=MWAA_ENV_NAME,
            environment_class=MWAA_ENV_CLASS,
            airflow_configuration_options={
                "core.load_default_connections": False,
                "core.load_examples": False,
                "webserver.dag_default_view": "tree",
                "webserver.dag_orientation": "TB",
                "core.lazy_load_plugins": False,
                "secrets.backend": "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend",
                "secrets.backend_kwargs": {
                    "connections_prefix": "airflow/connections",
                    "variables_prefix": "airflow/variables",
                },
            },
            dag_s3_path="dags",
            plugins_s3_path="plugins.zip",
            #plugins_s3_object_version=MWAA_PLUGINS_VERSION,
            requirements_s3_path="requirements.txt",
            #requirements_s3_object_version=MWAA_REQUIREMENTS_VERSION,
            source_bucket_arn=s3_bucket_mwaa.bucket_arn,
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                security_group_ids=[AIRFLOW_SG.security_group_id],
                subnet_ids=VPC.select_subnets(
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_NAT,
                ).subnet_ids[:2],
            ),
            execution_role_arn=airflow_role.role_arn,
            max_workers=10,
            webserver_access_mode="PUBLIC_ONLY",
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                task_logs={"enabled": True, "logLevel": "INFO"},
                worker_logs={"enabled": True, "logLevel": "INFO"},
                scheduler_logs={"enabled": True, "logLevel": "INFO"},
                dag_processing_logs={"enabled": True, "logLevel": "INFO"},
                webserver_logs={"enabled": True, "logLevel": "INFO"},
            ),
        )

        # output the airflow ux
        CfnOutput(
            self,
            "MWAAWebserverUrl",
            value=f"https://{airflow_env.attr_webserver_url}/home",
            export_name="mwaa-webserver-url",
        )
