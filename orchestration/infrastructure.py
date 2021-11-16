# import modules
from constructs import Construct
from aws_cdk import (
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


class MWAALocalRunner(Stack):

    """
    create instance for local runner and install it
    open ports to lineage and redshift
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        # OPENLINEAGE_SG: ec2.SecurityGroup,
        # REDSHIFT_SG: ec2.SecurityGroup,
        VPC=ec2.Vpc,
        EXTERNAL_IP=None,
    ):
        super().__init__(scope, id)

        # sg for the instance
        localrunner_sg = ec2.SecurityGroup(
            self, "localrunner_sg", vpc=VPC, description="local runner instance sg"
        )

        # Open port 22 for SSH
        for port in [22]:
            localrunner_sg.add_ingress_rule(
                ec2.Peer.ipv4(f"{EXTERNAL_IP}/32"),
                ec2.Port.tcp(port),
                "local runner from external ip",
            )

        # OPENLINEAGE_SG.connections.allow_from(
        #     localrunner_sg, ec2.Port.tcp(5000), "local runner to Openlineage"
        # )

        # REDSHIFT_SG.connections.allow_from(
        #     localrunner_sg, ec2.Port.tcp(5439), "local runner to Redshift"
        # )

        # role for instance
        localrunner_instance_role = iam.Role(
            self,
            "localrunner_instance_role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchAgentServerPolicy"
                ),
            ],
        )
        # instance for dev
        localrunner_instance = ec2.Instance(
            self,
            "dev_instance",
            instance_type=ec2.InstanceType("t2.xlarge"),
            machine_image=ec2.AmazonLinuxImage(
                generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2
            ),
            vpc=VPC,
            vpc_subnets={"subnet_type": ec2.SubnetType.PUBLIC},
            key_name="newKeyPair",
            role=localrunner_instance_role,
            security_group=localrunner_sg,
            init=ec2.CloudFormationInit.from_config_sets(
                config_sets={"default": ["prereqs"]},
                # order: packages -> groups -> users-> sources -> files -> commands -> services
                configs={
                    "prereqs": ec2.InitConfig(
                        [
                            # update yum
                            ec2.InitCommand.shell_command("yum update -y"),
                            ec2.InitCommand.shell_command("yum upgrade -y"),
                            ec2.InitCommand.shell_command("yum install -y awslogs"),
                            ec2.InitCommand.shell_command("systemctl start awslogsd"),
                            ec2.InitCommand.shell_command(
                                "amazon-linux-extras install epel"
                            ),
                            # push logs to cloudwatch with agent
                            ec2.InitPackage.yum("amazon-cloudwatch-agent"),
                            # ec2.InitService.enable("amazon-cloudwatch-agent"),
                            # missing setup here to export aws logs?
                            ec2.InitCommand.shell_command(
                                "/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s"
                            ),
                            # need git to clone marquez
                            ec2.InitPackage.yum("git"),
                            # pre-requisites for marquez
                            ec2.InitPackage.yum("docker"),
                            ec2.InitService.enable("docker"),
                            # install pip to get docker-compose
                            ec2.InitCommand.shell_command(
                                "yum -y install python-pip",
                            ),
                            ec2.InitCommand.shell_command(
                                "python3 -m pip install docker-compose",
                            ),
                            ec2.InitCommand.shell_command(
                                "ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose"
                            ),
                            # add ec2-user to docker group
                            ec2.InitCommand.shell_command(
                                "usermod -aG docker ec2-user",
                            ),
                            # kick the groups to add ec2-user to docker
                            ec2.InitCommand.shell_command("sudo -u ec2-user newgrp"),
                        ]
                    ),
                },
            ),
            init_options={
                "config_sets": ["default"],
                "timeout": Duration.minutes(30),
            },
        )
        # create Outputs
        CfnOutput(
            self,
            "LocalRunnerSSH",
            value=f"ssh -i ~/Downloads/newKeyPair.pem ec2-user@{localrunner_instance.instance_public_dns_name}",
            export_name="localrunner-ssh",
        )


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
        # OPENLINEAGE_SG: ec2.SecurityGroup,
        # REDSHIFT_SG: ec2.SecurityGroup,
        MWAA_REQUIREMENTS_VERSION: str,
        MWAA_PLUGINS_VERSION: str,
        VPC=ec2.Vpc,
        MWAA_DEPLOY_FILES: bool = False,
    ):
        super().__init__(scope, id)

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
        )
        # tag the bucket
        Tags.of(s3_bucket_mwaa).add("purpose", "MWAA")

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

        # deploy files to mwaa bucket
        if MWAA_DEPLOY_FILES:
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
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "SecretsManagerReadWrite"
                )
            ],
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
                            resources=[f"arn:aws:sqs:{Aws.REGION}:*:airflow-celery-*"],
                        ),
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

        # add path to openlineage
        # OPENLINEAGE_SG.connections.allow_from(
        #    airflow_sg, ec2.Port.tcp(5000), "MWAA to Openlineage"
        # )
        # add path to redshift
        # REDSHIFT_SG.connections.allow_from(
        #    airflow_sg, ec2.Port.tcp(5439), "MWAA to Redshift"
        # )

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
                "secrets.backend": "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend",
                "secrets.backend_kwargs": {
                    "connections_prefix": "airflow/connections",
                    "variables_prefix": "airflow/variables",
                },
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
        if MWAA_DEPLOY_FILES:
            airflow_env.node.add_dependency(airflow_files)

        # create secrets for MWAA config
        # mwaa_secret_lineage_backend = _sm.Secret(
        #    self,
        #    "mwaa_secret_lineage_backend",
        #    description="MWAA lineage backend",
        #    secret_name="airflow/variables/AIRFLOW__LINEAGE__BACKEND",
        #    # secret_value = "openlineage.lineage_backend.OpenLineageBackend"
        #    removal_policy=RemovalPolicy.DESTROY,
        # )
        # mwaa_secret_openlineage_url = _sm.Secret(
        #    self,
        #    "mwaa_secret_openlineage_url",
        #    description="Openlineage url",
        #    secret_name="airflow/variables/OPENLINEAGE_URL",
        #    # secret_value = "openlineage.lineage_backend.OpenLineageBackend"
        #    removal_policy=RemovalPolicy.DESTROY,
        # )
        # mwaa_secret_openlineage_namespace = _sm.Secret(
        #    self,
        #    "mwaa_secret_openlineage_namespace",
        #    description="Openlineage namespace",
        #    secret_name="airflow/variables/OPENLINEAGE_NAMESPACE",
        #    # secret_value = "openlineage.lineage_backend.OpenLineageBackend"
        #    removal_policy=RemovalPolicy.DESTROY,
        # )
        #
        #        # create the connection string for MWAA to Redshift
        #        mwaa_redshift_connection_string = _sm.Secret(
        #            self,
        #            "mwaa_redshift_connection_string",
        #            description="MWAA Redshift Connection",
        #            secret_name="airflow/connections/REDSHIFT_CONNECTOR",
        #            removal_policy=RemovalPolicy.DESTROY,
        #        )

        CfnOutput(
            self,
            "MWAAWebserverUrl",
            value=f"https://{airflow_env.attr_webserver_url}/home",
            export_name="mwaa-webserver-url",
        )
