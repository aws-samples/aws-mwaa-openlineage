# modules
from aws_cdk import (
    core,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
)
from typing import Optional, List
from scripts.s3customeresource import CustomResource
from pathlib import Path

# set path
dirname = Path(__file__).parent


class S3BucketDeletable(s3.Bucket):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        block_public_access: Optional[s3.BlockPublicAccess] = None,
        bucket_name: Optional[str] = None,
        encryption: Optional[s3.BucketEncryption] = None,
        public_read_access: Optional[bool] = None,
        removal_policy: Optional[core.RemovalPolicy] = None,
        server_access_logs_bucket: Optional[s3.IBucket] = None,
        server_access_logs_prefix: Optional[str] = None,
        versioned: Optional[bool] = None,
        **kwargs,
    ) -> None:
        removal_policy = removal_policy or core.RemovalPolicy.DESTROY
        super().__init__(self, id, **kwargs)


class S3BucketDelete(core.Construct):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        *,
        server_access_logs_bucket=None,
        project=None,
        purpose=None,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        # create the bucket
        self.bucket = s3.Bucket(
            self,
            "bucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            server_access_logs_bucket=server_access_logs_bucket,
        )
        if project:
            core.Tags.of(self.bucket).add("project", project)
        if purpose:
            core.Tags.of(self.bucket).add("purpose", purpose)

        # create the custom resource to empty
        bucket_empty = CustomResource(
            self,
            "bucket_empty",
            PhysicalId="scriptsBucketEmpty",
            Bucket=self.bucket,
        )
        # needs dependency to work in correct order
        bucket_empty.node.add_dependency(self.bucket)

        # lambda to delete the files
        # empty_bucket = lambda_.SingletonFunction(
        #    self,
        #    "empty_bucket",
        #    description="To delete files from the s3 bucket",
        #    uuid="7677dc81-117d-41c0-b75b-db11cb84bb70",
        #    code=lambda_.Code.from_asset(
        #        str(dirname.parent.joinpath("scripts/lambda"))
        #    ),
        #    handler="index.main",
        #    timeout=core.Duration.seconds(300),
        #    runtime=lambda_.Runtime.PYTHON_3_7,
        #    initial_policy=[
        #        iam.PolicyStatement(
        #            effect=iam.Effect.ALLOW,
        #            actions=["s3:ListBucket"],
        #            resources=["*"],
        #        ),
        #        iam.PolicyStatement(
        #            effect=iam.Effect.ALLOW,
        #            actions=[
        #                "s3:DeleteObject",
        #            ],
        #            resources=["*"],
        #        ),
        ##    ],
        #    log_retention=logs.RetentionDays.ONE_DAY,
        # )
        # needs dependency to work in correct order

    # set properties
    @property
    def bucket_arn(self):
        return self.bucket.bucket_arn

    @property
    def bucket_name(self):
        return self.bucket.bucket_name

    @property
    def Bucket(self):
        return self.bucket