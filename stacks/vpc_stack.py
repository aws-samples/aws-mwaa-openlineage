# import modules
from aws_cdk import (
    core,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_iam as iam,
)
from scripts.custom_resource import CustomResource
from pathlib import Path

# set path
dirname = Path(__file__).parent


class VpcStack(core.Stack):
    """ create the vpc
        create an s3 vpc endpoint
        create s3 buckets for scripts, data (raw, processed, serving), and logs
        create a custom function to empty the s3 buckets on destroy
    """

    def __init__(
        self, scope: core.Construct, id: str, constants: dict, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # create the vpc
        self.dl_vpc = ec2.Vpc(self, "dl_vpc")
        core.Tag.add(self.dl_vpc, "project", constants["PROJECT_TAG"])

        # add s3 endpoint
        self.dl_vpc.add_gateway_endpoint(
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

        # create s3 bucket for raw
        self.s3_bucket_raw = s3.Bucket(
            self,
            "s3_bucket_raw",
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        # tag the bucket
        core.Tag.add(self.s3_bucket_raw, "project", constants["PROJECT_TAG"])
        core.Tag.add(self.s3_bucket_raw, "purpose", "RAW")

        # create s3 bucket for processed
        self.s3_bucket_processed = s3.Bucket(
            self,
            "s3_bucket_processed",
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        # tag the bucket
        core.Tag.add(self.s3_bucket_processed, "project", constants["PROJECT_TAG"])
        core.Tag.add(self.s3_bucket_processed, "purpose", "PROCESSED")

        # create s3 bucket for serving
        self.s3_bucket_serving = s3.Bucket(
            self,
            "s3_bucket_serving",
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        # tag the bucket
        core.Tag.add(self.s3_bucket_serving, "project", constants["PROJECT_TAG"])
        core.Tag.add(self.s3_bucket_serving, "purpose", "SERVING")

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
                    f"{self.s3_bucket_raw.bucket_arn}/*",
                    f"{self.s3_bucket_processed.bucket_arn}/*",
                    f"{self.s3_bucket_serving.bucket_arn}/*",
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
                self.s3_bucket_raw.bucket_name,
                self.s3_bucket_processed.bucket_name,
                self.s3_bucket_serving.bucket_name,
                self.s3_bucket_logs.bucket_name,
            ],
            ResourcePolicies=bucket_empty_policy,
        )
        # needs dependency to work in correct order
        s3_bucket_empty.node.add_dependency(self.s3_bucket_scripts)
        s3_bucket_empty.node.add_dependency(self.s3_bucket_raw)
        s3_bucket_empty.node.add_dependency(self.s3_bucket_processed)
        s3_bucket_empty.node.add_dependency(self.s3_bucket_serving)
        s3_bucket_empty.node.add_dependency(self.s3_bucket_logs)

    # properties
    @property
    def get_s3_bucket_scripts(self):
        return self.s3_bucket_scripts

    @property
    def get_s3_bucket_raw(self):
        return self.s3_bucket_raw

    @property
    def get_s3_bucket_processed(self):
        return self.s3_bucket_processed

    @property
    def get_s3_bucket_serving(self):
        return self.s3_bucket_serving

    @property
    def get_s3_bucket_logs(self):
        return self.s3_bucket_logs

    @property
    def get_vpc(self):
        return self.dl_vpc
