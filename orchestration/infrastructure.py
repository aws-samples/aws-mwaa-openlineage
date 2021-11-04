# import modules
from aws_cdk import (
    core as cdk,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_iam as iam,
    aws_logs as logs,
    aws_mwaa as mwaa,
)
from pathlib import Path

# set path
dirname = Path(__file__).parent


class MWAA(cdk.Stack):
    """
    create s3 buckets for mwaa
    create mwaa env
    """

    def __init__(
        self,
        scope: cdk.Construct,
        id: str,
        VPC=ec2.Vpc,
        MWAA_ENV_NAME: str = None,
        MWAA_ENV_CLASS: str = "mw1.small",
        OPENLINEAGE_URL: str = None,
        OPENLINEAGE_INSTANCE_SG: ec2.SecurityGroup = None,
        MWAA_REQUIREMENTS_VERSION: str = None,
        MWAA_PLUGINS_VERSION: str = None,
    ):
        super().__init__(scope, id)

        # create s3 bucket for mwaa
        s3_bucket_mwaa = s3.Bucket(
            self,
            "s3_bucket_mwaa",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
        )
        # tag the bucket
        cdk.Tags.of(s3_bucket_mwaa).add("purpose", "MWAA")

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

        # deploy requirements.txt to mwaa bucket
        airflow_files = s3_deploy.BucketDeployment(
            self,
            "deploy_requirements",
            destination_bucket=s3_bucket_mwaa,
            sources=[
                s3_deploy.Source.asset("./orchestration/runtime/mwaa/"),
            ],
            include=["requirements.txt", "plugins.zip", "dags/*"],
        )

        # role for mwaa
        airflow_role = iam.Role(
            self,
            "airflow_role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
                iam.ServicePrincipal("airflow.amazonaws.com"),
            ),
            inline_policies=[
                iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["airflow:PublishMetrics"],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.DENY,
                            actions=["s3:ListAllMyBuckets"],
                            resources=[
                                s3_bucket_mwaa.bucket_arn,
                                f"{s3_bucket_mwaa.bucket_arn}/*",
                            ],
                        ),
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
                        ),
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
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["cloudwatch:PutMetricData"],
                            resources=["*"],
                        ),
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
                            resources=[
                                f"arn:aws:sqs:{cdk.Aws.REGION}:*:airflow-celery-*"
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "kms:Decrypt",
                                "kms:DescribeKey",
                                "kms:GenerateDataKey*",
                                "kms:Encrypt",
                            ],
                            not_resources=[f"arn:aws:kms:*:{cdk.Aws.ACCOUNT_ID}:key/*"],
                            conditions={
                                "StringLike": {
                                    "kms:ViaService": [
                                        f"sqs.{cdk.Aws.REGION}.amazonaws.com"
                                    ]
                                }
                            },
                        ),
                    ]
                )
            ],
        )

        # mwaa security group
        airflow_sg = ec2.SecurityGroup(
            self, "airflow_sg", vpc=VPC, description="MWAA sg"
        )
        # add access within group
        airflow_sg.connections.allow_internally(ec2.Port.all_traffic(), "within MWAA")

        # add mwaa security groups to openlineage instance security group for ingress
        OPENLINEAGE_INSTANCE_SG.connections.allow_from(
            airflow_sg, ec2.Port.tcp(5000), "MWAA to Openlineage API"
        )

        # add an airflow environment
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
            },
            dag_s3_path="dags",
            plugins_s3_path="plugins.zip",
            plugins_s3_object_version=MWAA_PLUGINS_VERSION,
            requirements_s3_path="requirements.txt",
            requirements_s3_object_version=MWAA_REQUIREMENTS_VERSION,
            source_bucket_arn=s3_bucket_mwaa.bucket_arn,
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                security_group_ids=[airflow_sg.security_group_id],
                subnet_ids=VPC.select_subnets(
                    subnet_type=ec2.SubnetType.PRIVATE,
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
        # don't deploy until after requirements is done
        airflow_env.node.add_dependency(airflow_files)

        cdk.CfnOutput(
            self,
            "MWAAWebserverUrl",
            value=f"https://{airflow_env.attr_webserver_url}/home",
            export_name="mwaa-webserver-url",
        )

        # set output props
        self.output_props = {}
        self.output_props["s3_bucket_mwaa"] = s3_bucket_mwaa

    # properties
    @property
    def outputs(self):
        return self.output_props
