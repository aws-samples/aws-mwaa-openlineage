# import modules
from aws_cdk import (
    core,
    aws_cloudtrail as cloudtrail,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_iam as iam,
    aws_logs as logs,
)
from scripts.custom_resource import CustomResource
from pathlib import Path

# set path
dirname = Path(__file__).parent


class MwaaStack(core.Stack):
    """
    create s3 buckets for mwaa
    add mwaa s3 bucket to cloudtrail for s3 bucket logging
    """

    def __init__(
        self, scope: core.Construct, id: str, vpc_stack, constants: dict, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # create s3 bucket for airflow
        s3_bucket_mwaa = s3.Bucket(
            self,
            "s3_bucket_mwaaa",
            bucket_name="airflow-awscdkdl1306",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            server_access_logs_bucket=vpc_stack.output_props["s3_bucket_logs"],
        )
        core.Tags.of(s3_bucket_mwaa).add("project", constants["PROJECT_TAG"])
        core.Tags.of(s3_bucket_mwaa).add("purpose", "MWAA")

        # mwaa security group
        
        # mwaa iam role

        # add airflow to cloudtrail logs
        # vpc_stack.output_props["trail"].add_s3_event_selector(
        #    s3_selector=[
        #        cloudtrail.S3EventSelector(bucket=s3_bucket_mwaa),
        #    ]
        # )

        # empty the s3 buckets for deletion ################################################
        # lambda policies
        bucket_empty_policy = [
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:ListBucket"],
                resources=["*"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:DeleteObject",
                ],
                resources=[
                    f"{s3_bucket_mwaa.bucket_arn}/*",
                ],
            ),
        ]

        # create the custom resource to empty
        s3_bucket_empty = CustomResource(
            self,
            "s3_bucket_empty",
            PhysicalId="scriptsBucketEmpty",
            Description="Empty s3 buckets",
            Uuid="f7d4f730-4ee1-11e8-9c2d-fa7ae01mwaa",
            HandlerPath=str(dirname.parent.joinpath("scripts/s3_bucket_empty.py")),
            BucketNames=[
                s3_bucket_mwaa.bucket_arn,
            ],
            ResourcePolicies=bucket_empty_policy,
        )
        # needs dependency to work in correct order
        s3_bucket_empty.node.add_dependency(s3_bucket_mwaa)

        # set output props
        self.output_props = {}
        self.output_props["s3_bucket_mwaa"] = s3_bucket_mwaa

    # properties
    @property
    def outputs(self):
        return self.output_props
