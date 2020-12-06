# import modules
from aws_cdk import (
    core,
    aws_cloudtrail as cloudtrail,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_iam as iam,
    aws_logs as logs,
    aws_kinesisfirehose as firehose,
)
from scripts.custom_resource import CustomResource
from pathlib import Path

# set path
dirname = Path(__file__).parent
# aws_cdk.aws_kinesisfirehose.CfnDeliveryStream


class VpcStack(core.Stack):
    """create the vpc
    create an s3 vpc endpoint
    create an athena vpc endpoint
    create s3 buckets
        scripts
        raw
        processed
        serving
        athena
        logs
        mmwa
    create cloudtrail for s3 bucket logging
    create a custom function to empty the s3 buckets on destroy
    deploy file from scripts directory into the raw bucket
    """

    def __init__(
        self, scope: core.Construct, id: str, constants: dict, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # import the vpc from context
        try:
            vpc = ec2.Vpc.from_lookup(self, "vpc", vpc_id=constants["VPC_ID"])
        # if no vpc in context then create
        except KeyError:
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

        # create s3 bucket for logs
        s3_bucket_logs = s3.Bucket(
            self,
            "s3_bucket_logs",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        # tag the bucket
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
            server_access_logs_bucket=s3_bucket_logs,
        )
        # tag the bucket
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
            server_access_logs_bucket=s3_bucket_logs,
        )
        # tag the bucket
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
            server_access_logs_bucket=s3_bucket_logs,
        )
        # tag the bucket
        core.Tags.of(s3_bucket_processed).add("project", constants["PROJECT_TAG"])
        core.Tags.of(s3_bucket_processed).add("purpose", "PROCESSED")

        # create s3 bucket for serving
        s3_bucket_serving = s3.Bucket(
            self,
            "s3_bucket_serving",
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            server_access_logs_bucket=s3_bucket_logs,
        )
        # tag the bucket
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
            server_access_logs_bucket=s3_bucket_logs,
        )
        core.Tags.of(s3_bucket_logs).add("project", constants["PROJECT_TAG"])
        core.Tags.of(s3_bucket_logs).add("purpose", "ATHENA")

        # cloudtrail for object logs
        trail = cloudtrail.Trail(self, "dl_trail", bucket=s3_bucket_logs)
        trail.add_s3_event_selector(
            s3_selector=[
                cloudtrail.S3EventSelector(bucket=s3_bucket_scripts),
                cloudtrail.S3EventSelector(bucket=s3_bucket_raw),
                cloudtrail.S3EventSelector(bucket=s3_bucket_processed),
                cloudtrail.S3EventSelector(bucket=s3_bucket_serving),
                cloudtrail.S3EventSelector(bucket=s3_bucket_athena),
            ]
        )

        # enable reading scripts from s3 bucket
        firehose_s3_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:AbortMultipartUpload",
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:PutObject",
            ],
            resources=[s3_bucket_raw.bucket_arn, f"{s3_bucket_raw.bucket_arn}/*"],
        )
        firehose_policy_document = iam.PolicyDocument()
        firehose_policy_document.add_statements(firehose_s3_policy)

        # role for kinesis firehose
        firehose_role = iam.Role(
            self,
            "firehose_role",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            inline_policies=[firehose_policy_document],
        )

        # firehose log group
        firehose_log_group = logs.LogGroup(
            self, "firehose_log_group", removal_policy=core.RemovalPolicy.DESTROY
        )

        # firehose log stream
        firehose_log_stream = logs.LogStream(
            self,
            "firehose_log_stream",
            removal_policy=core.RemovalPolicy.DESTROY,
            log_group=firehose_log_group,
        )

        # kinesis firehose to receive data
        firehose_raw = firehose.CfnDeliveryStream(
            self,
            "firehose_raw",
            s3_destination_configuration=(
                firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                    bucket_arn=s3_bucket_raw.bucket_arn,
                    role_arn=firehose_role.role_arn,
                    cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                        enabled=True,
                        log_group_name=firehose_log_group.log_group_name,
                        log_stream_name=firehose_log_stream.log_stream_name,
                    ),
                    buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                        interval_in_seconds=60, size_in_m_bs=1
                    ),
                    prefix="firehose_raw/",
                )
            ),
        )
        core.Tags.of(firehose_raw).add("project", constants["PROJECT_TAG"])

        # deploy customer file to the raw bucket
        s3_deploy.BucketDeployment(
            self,
            "xyz",
            destination_bucket=s3_bucket_raw,
            sources=[
                s3_deploy.Source.asset("./scripts", exclude=["**", "!customer.tbl"])
            ],
        )

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
                    f"{s3_bucket_logs.bucket_arn}/*",
                    f"{s3_bucket_scripts.bucket_arn}/*",
                    f"{s3_bucket_raw.bucket_arn}/*",
                    f"{s3_bucket_processed.bucket_arn}/*",
                    f"{s3_bucket_serving.bucket_arn}/*",
                    f"{s3_bucket_athena.bucket_arn}/*",
                ],
            ),
        ]

        # create the custom resource to empty
        s3_bucket_empty = CustomResource(
            self,
            "s3_bucket_empty",
            PhysicalId="scriptsBucketEmpty",
            Description="Empty s3 buckets",
            Uuid="f7d4f730-4ee1-11e8-9c2d-fa7ae01bbebc",
            HandlerPath=str(dirname.parent.joinpath("scripts/s3_bucket_empty.py")),
            BucketNames=[
                s3_bucket_logs.bucket_name,
                s3_bucket_scripts.bucket_name,
                s3_bucket_raw.bucket_name,
                s3_bucket_processed.bucket_name,
                s3_bucket_serving.bucket_name,
            ],
            ResourcePolicies=bucket_empty_policy,
        )
        # needs dependency to work in correct order
        s3_bucket_empty.node.add_dependency(s3_bucket_logs)
        s3_bucket_empty.node.add_dependency(s3_bucket_scripts)
        s3_bucket_empty.node.add_dependency(s3_bucket_raw)
        s3_bucket_empty.node.add_dependency(s3_bucket_processed)
        s3_bucket_empty.node.add_dependency(s3_bucket_serving)
        s3_bucket_empty.node.add_dependency(s3_bucket_athena)

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
