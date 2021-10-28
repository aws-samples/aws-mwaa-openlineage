# import modules
from aws_cdk import (
    core as cdk,
    aws_ec2 as ec2,
    aws_iam as iam,
)
from pathlib import Path

# set path
dirname = Path(__file__).parent


class Lineage(cdk.Stack):
    """
    Deploy ec2 instance
    Clone marquez
    Run marquez
    """

    def __init__(
        self,
        scope: cdk.Construct,
        id: str,
        *,
        VPC: ec2.Vpc,
        EXTERNAL_IP: str,
        MARQUEZ_INSTANCE: ec2.InstanceType,
        KEY_PAIR: str,
    ):
        super().__init__(scope, id)

        # marquez sg
        mq_sg = ec2.SecurityGroup(self, "mq_sg", vpc=VPC)

        # Open port 22 for SSH
        for port in [22, 3000, 5000, 5001]:
            mq_sg.add_ingress_rule(
                ec2.Peer.ipv4(f"{EXTERNAL_IP}/32"),
                ec2.Port.tcp(port),
                "from own public ip",
            )

        # role for instance
        mq_instance_role = iam.Role(
            self,
            "mq_instance_role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchAgentServerPolicy"
                ),
            ],
        )

        # instance for lineage
        mq_instance = ec2.Instance(
            self,
            "marquez_instance",
            instance_type=MARQUEZ_INSTANCE,
            machine_image=ec2.AmazonLinuxImage(
                generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2
            ),
            vpc=VPC,
            vpc_subnets={"subnet_type": ec2.SubnetType.PUBLIC},
            key_name=KEY_PAIR,
            role=mq_instance_role,
            security_group=mq_sg,
            init=ec2.CloudFormationInit.from_config_sets(
                config_sets={"default": ["prereqs", "marquez"]},
                # order: packages -> groups -> users-> sources -> files -> commands -> services
                configs={
                    "prereqs": ec2.InitConfig(
                        [
                            # update yum
                            ec2.InitCommand.shell_command("yum update -y"),
                            ec2.InitCommand.shell_command("yum upgrade -y"),
                            ec2.InitCommand.shell_command("yum install -y awslogs"),
                            ec2.InitCommand.shell_command("systemctl start awslogsd"),
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
                                "sudo -u ec2-user python3 -m pip install docker-compose",
                            ),
                            ec2.InitCommand.shell_command(
                                '''curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose'''
                            ),
                            ec2.InitCommand.shell_command(
                                '''curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose'''
                            ),
                            # trust github - not secure for prod
                            ec2.InitCommand.shell_command(
                                "chmod +x /usr/local/bin/docker-compose"
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
                            # check docker-compose
                            ec2.InitCommand.shell_command(
                                "sudo -u ec2-user docker-compose",
                                ignore_errors=True,
                            ),
                            # clone marquez
                            ec2.InitCommand.shell_command(
                                "sudo -u ec2-user git clone https://github.com/MarquezProject/marquez.git /home/ec2-user/marquez"
                            ),
                            # start marquez
                            # start not working as docker compose not recognized?
                            ec2.InitCommand.shell_command(
                                "sudo -u ec2-user ./docker/up.sh",
                                cwd="/home/ec2-user/marquez",
                                ignore_errors=True,
                            ),
                        ]
                    ),
                },
            ),
            init_options={
                "config_sets": ["default"],
                "timeout": cdk.Duration.minutes(30),
            },
        )

        # create Outputs
        cdk.CfnOutput(
            self,
            "MarquezInstanceSSH",
            value=f"ssh -i ~/Downloads/newKeyPair.pem {mq_instance.instance_public_dns_name}",
            export_name="marquez-instance-ssh",
        )
        cdk.CfnOutput(
            self,
            "MarquezInstanceUI",
            value=f"http://{mq_instance.instance_public_dns_name}:3000",
            export_name="marquez-instance-ui",
        )

        # marquez address
        self.MARQUEZ_URL = mq_instance.instance_public_dns_name

        # set output props
        self.output_props = {}
        self.output_props["mq_instance"] = mq_instance
        self.output_props["mq_public_dns"] = mq_instance.instance_public_dns_name

    # properties
    @property
    def outputs(self):
        return self.output_props
