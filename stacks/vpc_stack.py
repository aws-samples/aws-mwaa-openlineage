# import modules
from aws_cdk import (
    core,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
)
from scripts.custom_resource import CustomResource
from scripts.constants import constants
from pathlib import Path

# set path
dirname = Path(__file__).parent


class VpcStack(core.Stack):
    """ create the vpc
        create an s3 vpc endpoint
        create s3 buckets for scripts, data, and logs
        create a custom function to empty the s3 buckets on destroy
    """

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # create the vpc
        self.emr_vpc = ec2.Vpc(self, "emr_vpc")
        core.Tag.add(self.emr_vpc, "project", constants["PROJECT_TAG"])

        # add s3 endpoint
        self.emr_vpc.add_gateway_endpoint(
            "e6ad3311-f566-426e-8291-6937101db6a1",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # create s3 bucket for scripts
        self.s3_bucket_scripts = s3.Bucket(
            self,
            "s3_bucket_scripts",
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        # tag the bucket
        core.Tag.add(self.s3_bucket_scripts, "project", constants["PROJECT_TAG"])
        core.Tag.add(self.s3_bucket_scripts, "purpose", "SCRIPTS")

        # create s3 bucket for data
        self.s3_bucket_data = s3.Bucket(
            self,
            "s3_bucket_data",
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        # tag the bucket
        core.Tag.add(self.s3_bucket_data, "project", constants["PROJECT_TAG"])
        core.Tag.add(self.s3_bucket_data, "purpose", "DATA")

        # create s3 bucket for logs
        self.s3_bucket_logs = s3.Bucket(
            self,
            "s3_bucket_logs",
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        # tag the bucket
        core.Tag.add(self.s3_bucket_logs, "project", constants["PROJECT_TAG"])
        core.Tag.add(self.s3_bucket_logs, "purpose", "LOGS")

        # lambda policies
        bucket_empty_policy = [
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW, actions=["s3:ListBucket"], resources=["*"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:DeleteObject",],
                resources=[
                    f"{self.s3_bucket_scripts.bucket_arn}/*",
                    f"{self.s3_bucket_data.bucket_arn}/*",
                    f"{self.s3_bucket_logs.bucket_arn}/*",
                ],
            ),
        ]

        # create the custom resource to date scripts buckets
        s3_bucket_empty = CustomResource(
            self,
            "s3_bucket_empty",
            PhysicalId="scriptsBucketEmpty",
            Description="Empty s3 buckets",
            Uuid="f7d4f730-4ee1-11e8-9c2d-fa7ae01bbebc",
            HandlerPath=str(dirname.parent.joinpath("scripts/s3_bucket_empty.py")),
            BucketNames=[
                self.s3_bucket_scripts.bucket_name,
                self.s3_bucket_data.bucket_name,
                self.s3_bucket_logs.bucket_name,
            ],
            ResourcePolicies=bucket_empty_policy,
        )
        # needs dependency to work in correct order
        s3_bucket_empty.node.add_dependency(self.s3_bucket_data)
        s3_bucket_empty.node.add_dependency(self.s3_bucket_logs)
        s3_bucket_empty.node.add_dependency(self.s3_bucket_scripts)

        # emr job flow role
        self.emr_jobflow_role = iam.Role(
            self,
            "emr_jobflow_role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonElasticMapReduceforEC2Role"
                )
            ],
        )
        # emr job flow profile
        self.emr_jobflow_profile = iam.CfnInstanceProfile(
            self,
            "emr_jobflow_profile",
            roles=[self.emr_jobflow_role.role_name],
            instance_profile_name=self.emr_jobflow_role.role_name,
        )

        # emr service role
        self.emr_service_role = iam.Role(
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

        # table for job specs
        jobs_table = dynamodb.Table(
            self,
            "jobs_table",
            partition_key=dynamodb.Attribute(
                name="id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY,
        )

    # properties
    @property
    def get_s3_bucket_scripts(self):
        return self.s3_bucket_scripts

    @property
    def get_s3_bucket_data(self):
        return self.s3_bucket_data

    @property
    def get_s3_bucket_logs(self):
        return self.s3_bucket_logs

    @property
    def get_vpc(self):
        return self.emr_vpc

    @property
    def get_vpc_public_subnet_ids(self):
        return self.emr_vpc.select_subnets(subnet_type=ec2.SubnetType.PUBLIC).subnet_ids

    @property
    def get_vpc_private_subnet_ids(self):
        return self.emr_vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE
        ).subnet_ids

    @property
    def get_emr_service_role(self):
        return self.emr_service_role

    @property
    def get_emr_jobflow_role(self):
        return self.emr_jobflow_role

    @property
    def get_emr_jobflow_profile(self):
        return self.emr_jobflow_profile
