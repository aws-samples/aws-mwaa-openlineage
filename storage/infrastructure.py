# import modules
from constructs import Construct
from aws_cdk import (
    aws_cloudtrail as cloudtrail,
    aws_ec2 as ec2,
    aws_s3 as s3,
    CfnOutput,
    RemovalPolicy,
    Stack,
    Tags,
)
from pathlib import Path

# set path
dirname = Path(__file__).parent



class S3(Stack):
    """
    create the vpc
    create an s3 vpc endpoint
    create s3 buckets
        scripts
        raw
        stage
        analytics
        logs
    create cloudtrail for s3 bucket logging
    create a custom function to empty the s3 buckets on destroy
    deploy file from scripts directory into the raw bucket
    """

    def __init__(
            self, 
            scope: Construct, 
            id: str, 
            EXTERNAL_IP: str,
            **kwargs
        ) -> None:

        super().__init__(scope, id, **kwargs)

        # create the vpc
        vpc = ec2.Vpc(self, "vpc", max_azs=3)

        # add s3 endpoint
        vpc.add_gateway_endpoint(
            "e6ad3311-f566-426e-8291-6937101db6a1",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        redshift_sg = ec2.SecurityGroup(
            self, 
            "redshift_sg", 
            vpc=vpc, 
            description="Redshift sg"
        )

        # lineage sg
        lineage_sg = ec2.SecurityGroup(
            self, 
            "lineage_sg", 
            vpc=vpc, 
            description="OpenLineage instance sg"
        )

        # # mwaa security group
        airflow_sg = ec2.SecurityGroup(
            self, 
            "airflow_sg", 
            vpc=vpc, 
            description="MWAA sg"
        )
        # add access within group
        airflow_sg.connections.allow_internally(ec2.Port.all_traffic(), "within MWAA")

        # Open port 22 for SSH
        for port in [22, 3000, 5000]:
            lineage_sg.add_ingress_rule(
                ec2.Peer.ipv4(f"{EXTERNAL_IP}/32"),
                ec2.Port.tcp(port),
                "Lineage from external ip",
            )

        lineage_sg.connections.allow_from(airflow_sg, ec2.Port.tcp(5000))
        redshift_sg.connections.allow_from(airflow_sg, ec2.Port.tcp(5439))
        redshift_sg.connections.allow_from(lineage_sg, ec2.Port.tcp(5439))


        # create s3 bucket for logs
        s3_bucket_logs = s3.Bucket(
            self,
            "logs",
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
            "raw",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        Tags.of(s3_bucket_raw).add("purpose", "RAW")

        # create s3 bucket for stage
        s3_bucket_stage = s3.Bucket(
            self,
            "stage",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        Tags.of(s3_bucket_stage).add("purpose", "STAGE")

        # create s3 bucket for analytics
        s3_bucket_analytics = s3.Bucket(
            self,
            "analytics",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=s3_bucket_logs,
        )
        Tags.of(s3_bucket_analytics).add("purpose", "ANALYTICS")

        # cloudtrail for object logs
        trail = cloudtrail.Trail(self, "dl_trail", bucket=s3_bucket_logs)
        trail.add_s3_event_selector(
            s3_selector=[
                cloudtrail.S3EventSelector(bucket=s3_bucket_raw),
                cloudtrail.S3EventSelector(bucket=s3_bucket_stage),
                cloudtrail.S3EventSelector(bucket=s3_bucket_analytics),
            ]
        )

        # to share ...
        self.VPC = vpc
        self.S3_BUCKET_RAW = s3_bucket_raw
        self.S3_BUCKET_STAGE = s3_bucket_stage

        # share security groups
        self.REDSHIFT_SG = redshift_sg
        self.OPENLINEAGE_SG = lineage_sg
        self.AIRFLOW_SG = airflow_sg

        CfnOutput(
            self,
            "RedshiftSg",
            value=redshift_sg.security_group_id,
            export_name="redshift-sg",
        )

        CfnOutput(
            self,
            "OpenlineageSg",
            value=lineage_sg.security_group_id,
            export_name="openlineage-sg",
        )

        CfnOutput(
            self,
            "AirflowSg",
            value=airflow_sg.security_group_id,
            export_name="airflow-sg",
        )