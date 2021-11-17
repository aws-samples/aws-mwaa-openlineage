# import modules
from constructs import Construct
from aws_cdk import (
    aws_cloudtrail as cloudtrail,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_s3 as s3,
    CfnOutput,
    RemovalPolicy,
    Stack,
    Tags,
)
from pathlib import Path

# set path
dirname = Path(__file__).parent


class Storage(Stack):
    """
    create the vpc
    create an s3 vpc endpoint
    create an athena vpc endpoint
    create s3 buckets
        scripts
        raw
        processed
        serving
        athena
        logs
    create cloudtrail for s3 bucket logging
    create a custom function to empty the s3 buckets on destroy
    deploy file from scripts directory into the raw bucket
    """

    def __init__(self, scope: Construct, id: str, EXTERNAL_IP=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # create the vpc
        vpc = ec2.Vpc(self, "vpc", max_azs=3)

        # add s3 endpoint
        vpc.add_gateway_endpoint(
            "e6ad3311-f566-426e-8291-6937101db6a1",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # create s3 bucket for logs
        s3_bucket_logs = s3.Bucket(
            self,
            "s3_bucket_logs",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        Tags.of(s3_bucket_logs).add("purpose", "LOGS")

        # create s3 bucket for raw
        s3_bucket_raw = s3.Bucket(
            self,
            "s3_bucket_raw",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        Tags.of(s3_bucket_raw).add("purpose", "RAW")

        # create s3 bucket for processed
        s3_bucket_processed = s3.Bucket(
            self,
            "s3_bucket_processed",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        Tags.of(s3_bucket_processed).add("purpose", "PROCESSED")

        # create s3 bucket for servicing
        s3_bucket_serving = s3.Bucket(
            self,
            "s3_bucket_serving",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        Tags.of(s3_bucket_serving).add("purpose", "SERVING")

        # cloudtrail for object logs
        trail = cloudtrail.Trail(self, "dl_trail", bucket=s3_bucket_logs)
        trail.add_s3_event_selector(
            s3_selector=[
                cloudtrail.S3EventSelector(bucket=s3_bucket_raw),
                cloudtrail.S3EventSelector(bucket=s3_bucket_processed),
                cloudtrail.S3EventSelector(bucket=s3_bucket_serving),
            ]
        )

        # deploy customer file to the raw bucket
        # s3_deploy.BucketDeployment(
        #    self,
        #    "xyz",
        #    destination_bucket=s3_bucket_raw,
        #    sources=[
        #        s3_deploy.Source.asset("./scripts", exclude=["**", "!customer.tbl"])
        #    ],
        # )

        # container storage (ecr)
        #ecr_repo = ecr.Repository(
        #    self, "ecr_repo", removal_policy=RemovalPolicy.DESTROY
        #)

        self.VPC = vpc
        self.S3_BUCKET_RAW_ARN = s3_bucket_raw.bucket_arn
        self.S3_BUCKET_RAW_NAME = s3_bucket_raw.bucket_name
        self.S3_BUCKET_PROCESSED_ARN = s3_bucket_processed.bucket_arn
        self.S3_BUCKET_PROCESSED_NAME = s3_bucket_processed.bucket_name
        # self.ECR_REPO = ecr_repo

        # outputs
        CfnOutput(
            self,
            "nyc-taxi-copy",
            value=f"aws s3 cp s3://nyc-tlc/trip\ data/green_tripdata_2020-06.csv s3://{s3_bucket_raw.bucket_name}/nyctaxi/",
            export_name="nyc-taxi-copy",
        )
