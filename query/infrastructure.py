# import modules
from aws_cdk import core as cdk, aws_athena as athena, aws_ec2 as ec2, aws_s3 as s3


class Athena(cdk.Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: cdk.Construct,
        id: str,
        VPC=ec2.Vpc,
        ATHENA_CATALOG_NAME: str = None,
    ):
        super().__init__(scope, id)

        # create the vpc endpoint for athena
        VPC.add_interface_endpoint(
            "athena_endpoint",
            service=ec2.InterfaceVpcEndpointAwsService(name="athena"),
        )
        # create s3 bucket for athena results
        s3_bucket_athena = s3.Bucket(
            self,
            "s3_bucket_athena",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            #    server_access_logs_bucket=s3_bucket_logs,
        )
        # athena data catalog
        #athena_catalog = athena.CfnDataCatalog(
        #    self,
        #    "athena_catalog",
        #    name=ATHENA_CATALOG_NAME,
        #    type="GLUE",
        #    parameters={"catalog-id", cdk.Aws.ACCOUNT_ID},
        #)
