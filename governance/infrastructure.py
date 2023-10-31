# import modules
from constructs import Construct
from aws_cdk import (
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_lakeformation as lf,
    aws_secretsmanager as sm,
    CfnOutput,
    Duration,
    Stack,
    RemovalPolicy,
    SecretValue
)
from pathlib import Path

# set path
dirname = Path(__file__).parent


class Marquez(Stack):
    """
    Deploy ec2 instance
    Clone marquez
    Run marquez
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        VPC: ec2.Vpc,
        LINEAGE_INSTANCE: ec2.InstanceType,
        OPENLINEAGE_SG: ec2.SecurityGroup,
        OPENLINEAGE_NAMESPACE: str,
        **kwargs

    ):
        super().__init__(scope, id, **kwargs)

        # role for instance
        lineage_instance_role = iam.Role(
            self,
            "lineage_instance_role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchAgentServerPolicy"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
        )

        # instance for lineage
        lineage_instance = ec2.Instance(
            self,
            "lineage_instance",
            instance_type=LINEAGE_INSTANCE,
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            vpc=VPC,
            vpc_subnets={"subnet_type": ec2.SubnetType.PUBLIC},
            role=lineage_instance_role,
            security_group=OPENLINEAGE_SG,
            detailed_monitoring=True,
            init=ec2.CloudFormationInit.from_config_sets(
                config_sets={"default": ["prereqs", "marquez"]},
                # order: packages -> groups -> users-> sources -> files -> commands -> services
                configs={
                    "prereqs": ec2.InitConfig(
                        [
                            # update yum
                            ec2.InitPackage.yum("git"),
                            # pre-requisites for marquez
                            ec2.InitPackage.yum("docker"),
                            ec2.InitService.enable("docker"),
                            ec2.InitCommand.shell_command(
                                "mkdir -p /usr/local/lib/docker/cli-plugins/",
                            ),
                            ec2.InitCommand.shell_command(
                                "curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose"
                            ),
                            ec2.InitCommand.shell_command(
                                "chmod +x /usr/local/lib/docker/cli-plugins/docker-compose"
                            ),
                            # add ec2-user to docker group
                            ec2.InitCommand.shell_command(
                                "usermod -aG docker ec2-user",
                            ),
                            # kick the groups to add ec2-user to docker
                            ec2.InitCommand.shell_command("sudo -u ec2-user newgrp"),
                        ]
                    ),
                    "marquez": ec2.InitConfig(
                        [
                            # check docker-compose version
                            ec2.InitCommand.shell_command(
                                "sudo -u ec2-user docker compose version",
                                ignore_errors=True,
                            ),
                            # clone marquez
                            ec2.InitCommand.shell_command(
                                "sudo -u ec2-user git clone https://github.com/MarquezProject/marquez.git /home/ec2-user/marquez"
                            ),
                            # start marquez
                            # start not working as docker compose not recognized?
                            ec2.InitCommand.shell_command(
                                "sudo -u ec2-user ./docker/up.sh --tag 0.42.0 --detach",
                                cwd="/home/ec2-user/marquez",
                                ignore_errors=True,
                            ),
                        ]
                    ),
                },
            ),
            init_options={
                "config_sets": ["default"],
                "timeout": Duration.minutes(30),
            },
        )

        # attributes to share
        self.OPENLINEAGE_URL = lineage_instance.instance_public_dns_name
        self.OPENLINEAGE_API = f"http://{lineage_instance.instance_public_dns_name}:5000"
        
        secret_openlineage_namespace = sm.Secret(
            self,
            "openlineage_namespace",
            description="Openlineage Namespace",
            secret_name="airflow/variables/OPENLINEAGE_NAMESPACE",
            secret_string_value=SecretValue.unsafe_plain_text(OPENLINEAGE_NAMESPACE),
            removal_policy=RemovalPolicy.DESTROY,
        )

        secret_openlineage_url = sm.Secret(
            self,
            "openlineage_url",
            description="Openlineage URL",
            secret_name="airflow/variables/OPENLINEAGE_URL",
            secret_string_value=SecretValue.unsafe_plain_text(f"http://{lineage_instance.instance_public_dns_name}:5000"),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # create Outputs
        CfnOutput(
            self,
            "LineageUI",
            value=f"http://{lineage_instance.instance_public_dns_name}:3000",
            export_name="lineage-ui",
        )
        CfnOutput(
            self,
            "OpenlineageApi",
            value=f"http://{lineage_instance.instance_public_dns_name}:5000",
            export_name="openlineage-api",
        )

