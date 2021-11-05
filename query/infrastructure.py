# import modules
from aws_cdk import (
    core as cdk,
    aws_athena as athena,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_redshift as redshift,
    aws_secretsmanager as _sm,
)


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


class Redshift(cdk.Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: cdk.Construct,
        id: str,
        REDSHIFT_DB_NAME: str,
        REDSHIFT_NUM_NODES: str,
        REDSHIFT_NODE_TYPE: str,
        REDSHIFT_CLUSTER_TYPE: str,
        REDSHIFT_MASTER_USERNAME: str,
        VPC=ec2.Vpc,
    ):
        super().__init__(scope, id)

        redshift_sg = ec2.SecurityGroup(
            self, "redshift_sg", vpc=VPC, description="Redshift sg"
        )

        redshift_password = _sm.Secret(
            self,
            "redshift_password",
            description="Redshift password",
            secret_name="REDSHIFT_PASSWORD",
            generate_secret_string=_sm.SecretStringGenerator(exclude_punctuation=True),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        redshift_cluster_role = iam.Role(
            self,
            "redshift_cluster_role",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ],
        )

        spectrum_lake_formation_policy = iam.ManagedPolicy(
            self,
            "spectrum_lake_formation_policy",
            description="Provide access between Redshift Spectrum and Lake Formation",
            managed_policy_name="RedshiftSpectrumPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "glue:CreateDatabase",
                        "glue:DeleteDatabase",
                        "glue:GetDatabase",
                        "glue:GetDatabases",
                        "glue:UpdateDatabase",
                        "glue:CreateTable",
                        "glue:DeleteTable",
                        "glue:BatchDeleteTable",
                        "glue:UpdateTable",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:BatchCreatePartition",
                        "glue:CreatePartition",
                        "glue:DeletePartition",
                        "glue:BatchDeletePartition",
                        "glue:UpdatePartition",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "glue:BatchGetPartition",
                        "lakeformation:GetDataAccess",
                    ],
                    resources=["*"],
                )
            ],
            roles=[redshift_cluster_role],
        )

        redshift_cluster_subnet_group = redshift.CfnClusterSubnetGroup(
            self,
            "redshiftDemoClusterSubnetGroup",
            subnet_ids=VPC.select_subnets(
                subnet_type=ec2.SubnetType.PRIVATE
            ).subnet_ids,
            description="Redshift Demo Cluster Subnet Group",
        )

        redshift_cluster = redshift.CfnCluster(
            self,
            "redshift_cluster",
            db_name=REDSHIFT_DB_NAME,
            cluster_type=REDSHIFT_CLUSTER_TYPE,
            number_of_nodes=REDSHIFT_NUM_NODES,
            master_username=REDSHIFT_MASTER_USERNAME,
            master_user_password=redshift_password.secret_value.to_string(),
            iam_roles=[redshift_cluster_role.role_arn],
            node_type=REDSHIFT_NODE_TYPE,
            cluster_subnet_group_name=redshift_cluster_subnet_group.ref,
            vpc_security_group_ids=[redshift_sg.security_group_id],
        )

        # self ...
        self.REDSHIFT_SG = redshift_sg

        # outputs
        output_1 = cdk.CfnOutput(
            self,
            "RedshiftCluster",
            value=f"{redshift_cluster.attr_endpoint_address}",
            description=f"RedshiftCluster Endpoint",
        )
        output_2 = cdk.CfnOutput(
            self,
            "RedshiftMasterUser",
            value=REDSHIFT_MASTER_USERNAME,
            description=f"Redshift master username",
        )

        output_3 = cdk.CfnOutput(
            self,
            "RedshiftClusterPassword",
            value=(
                f"https://console.aws.amazon.com/secretsmanager/home?region="
                f"{cdk.Aws.REGION}"
                f"#/secret?name="
                f"{redshift_password.secret_arn}"
            ),
            description=f"Redshift Cluster Password in Secrets Manager",
        )
