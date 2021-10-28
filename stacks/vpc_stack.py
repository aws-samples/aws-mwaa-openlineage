# import modules
from aws_cdk import (
    core,
    aws_cloudtrail as cloudtrail,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
)
from pathlib import Path
from bucket_cleaner.custom_resource import BucketCleaner

# set path
dirname = Path(__file__).parent


class VpcStack(core.Stack):
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

    def __init__(
        self, scope: core.Construct, id: str, constants: dict, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # create the vpc
        vpc = ec2.Vpc(self, "vpc", max_azs=3)
        # tag the vpc
        core.Tags.of(vpc).add("project", constants["PROJECT_TAG"])

        # add s3 endpoint
        vpc.add_gateway_endpoint(
            "e6ad3311-f566-426e-8291-6937101db6a1",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # add athena endpoint
        vpc.add_interface_endpoint(
            "athena_endpoint",
            service=ec2.InterfaceVpcEndpointAwsService(name="athena"),
        )

        # create the s3 buckets for the data environment
        # create s3 bucket for logs
        s3_bucket_logs = s3.Bucket(
            self,
            "s3_bucket_logs",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        core.Tags.of(s3_bucket_logs).add("project", constants["PROJECT_TAG"])
        core.Tags.of(s3_bucket_logs).add("purpose", "LOGS")

        # create s3 bucket for scripts
        s3_bucket_scripts = s3.Bucket(
            self,
            "s3_bucket_scripts",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        core.Tags.of(s3_bucket_scripts).add("project", constants["PROJECT_TAG"])
        core.Tags.of(s3_bucket_scripts).add("purpose", "SCRIPTS")

        # create s3 bucket for raw
        s3_bucket_raw = s3.Bucket(
            self,
            "s3_bucket_raw",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        core.Tags.of(s3_bucket_raw).add("project", constants["PROJECT_TAG"])
        core.Tags.of(s3_bucket_raw).add("purpose", "RAW")

        # create s3 bucket for processed
        s3_bucket_processed = s3.Bucket(
            self,
            "s3_bucket_processed",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        core.Tags.of(s3_bucket_processed).add("project", constants["PROJECT_TAG"])
        core.Tags.of(s3_bucket_processed).add("purpose", "PROCESSED")

        # create s3 bucket for servicing
        s3_bucket_serving = s3.Bucket(
            self,
            "s3_bucket_serving",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        core.Tags.of(s3_bucket_serving).add("project", constants["PROJECT_TAG"])
        core.Tags.of(s3_bucket_serving).add("purpose", "SERVING")

        # create s3 bucket for athena results
        s3_bucket_athena = s3.Bucket(
            self,
            "s3_bucket_athena",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )

        # cloudtrail for object logs
        trail = cloudtrail.Trail(self, "dl_trail", bucket=s3_bucket_logs)
        trail.add_s3_event_selector(
            s3_selector=[
                cloudtrail.S3EventSelector(bucket=s3_bucket_scripts),
                cloudtrail.S3EventSelector(bucket=s3_bucket_raw),
                cloudtrail.S3EventSelector(bucket=s3_bucket_processed),
                cloudtrail.S3EventSelector(bucket=s3_bucket_athena),
            ]
        )

        # deploy customer file to the raw bucket
        s3_deploy.BucketDeployment(
            self,
            "xyz",
            destination_bucket=s3_bucket_raw,
            sources=[
                s3_deploy.Source.asset("./scripts", exclude=["**", "!customer.tbl"])
            ],
        )

        # cleaner action on delete
        #s3_bucket_cleaner = BucketCleaner(
        #    self,
        #    "s3_bucket_cleaner",
        #    buckets=[
        #        s3_bucket_logs,
        #        s3_bucket_scripts,
        #        s3_bucket_raw,
        #        s3_bucket_processed,
        #        s3_bucket_athena,
        #    ],
        #    lambda_description=f"On delete empty {core.Stack.stack_name} S3 buckets",
        #)
        ##s3_bucket_cleaner.node.add_dependency(s3_bucket_logs)
        #s3_bucket_cleaner.node.add_dependency(s3_bucket_scripts)
        #s3_bucket_cleaner.node.add_dependency(s3_bucket_raw)
        ##s3_bucket_cleaner.node.add_dependency(s3_bucket_processed)
        #s3_bucket_cleaner.node.add_dependency(s3_bucket_athena)

        # set output props
        self.output_props = {}
        self.output_props["vpc"] = vpc
        self.output_props["trail"] = trail
        self.output_props["s3_bucket_logs"] = s3_bucket_logs
        self.output_props["s3_bucket_scripts"] = s3_bucket_scripts
        self.output_props["s3_bucket_raw"] = s3_bucket_raw
        self.output_props["s3_bucket_processed"] = s3_bucket_processed
        self.output_props["s3_bucket_serving"] = s3_bucket_serving
        self.output_props["s3_bucket_athena"] = s3_bucket_athena

    # properties
    @property
    def outputs(self):
        return self.output_props
