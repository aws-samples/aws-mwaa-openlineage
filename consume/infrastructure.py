# import modules
from constructs import Construct
from aws_cdk import (
    aws_athena as athena,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_s3 as s3,
    aws_redshift as redshift,
    aws_secretsmanager as _sm,
    Aws,
    CfnOutput,
    RemovalPolicy,
    Stack
)


class Athena(Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        VPC=ec2.Vpc,
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
            "athena",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # event rule to send athena events
        #glue_table_events = events.Rule(
        #    self,
        #    "glue_table_events",
        #    description="Glue table updates to openlineage",
        #    targets=[targets.LambdaFunction(athena_lineage_lambda)],
        #    event_pattern={
        #        "source": ["aws.glue"],
        #        "detail_type": ["Glue Data Catalog Table State Change"],
        #    },
        #)

class Redshift(Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        REDSHIFT_DB_NAME: str,
        REDSHIFT_NUM_NODES: str,
        REDSHIFT_NODE_TYPE: str,
        REDSHIFT_CLUSTER_TYPE: str,
        REDSHIFT_MASTER_USERNAME: str,
        REDSHIFT_SG: ec2.SecurityGroup,
        VPC=ec2.Vpc,
    ):
        super().__init__(scope, id)

        redshift_password = _sm.Secret(
            self,
            "redshift_password",
            description="Redshift password",
            secret_name="REDSHIFT_PASSWORD",
            generate_secret_string=_sm.SecretStringGenerator(exclude_punctuation=True),
            removal_policy=RemovalPolicy.DESTROY,
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
                subnet_type=ec2.SubnetType.PRIVATE_WITH_NAT
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
            vpc_security_group_ids=[REDSHIFT_SG.security_group_id],
        )

        # outputs
        output_1 = CfnOutput(
            self,
            "RedshiftCluster",
            value=f"{redshift_cluster.attr_endpoint_address}",
            description=f"RedshiftCluster Endpoint",
        )
        output_2 = CfnOutput(
            self,
            "RedshiftMasterUser",
            value=REDSHIFT_MASTER_USERNAME,
            description=f"Redshift master username",
        )

        output_3 = CfnOutput(
            self,
            "RedshiftClusterPassword",
            value=(
                f"https://console.aws.amazon.com/secretsmanager/home?region="
                f"{Aws.REGION}"
                f"#/secret?name="
                f"{redshift_password.secret_arn}"
            ),
            description=f"Redshift Cluster Password in Secrets Manager",
        )
        
        output_4 = CfnOutput(
            self,
            "RedshiftConnectionString",
            value=(
                f"postgres://{REDSHIFT_MASTER_USERNAME}:<Password>"
                f"@{redshift_cluster.attr_endpoint_address}:5439"
                f"/{redshift_cluster.db_name}"
            ),
            description=f"Redshift Connection String",
        )

class SageMaker(Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        VPC=ec2.Vpc,
    ):
        super().__init__(scope, id)

        # create the vpc endpoint for sagemaker
        #VPC.add_interface_endpoint(
        #    "sagemaker",
        #    service=ec2.InterfaceVpcEndpointAwsService(name="sagemaker"),
        #)

        # create s3 bucket for athena results
        #s3_bucket_athena = s3.Bucket(
        #    self,
        #    "athena",
        #    encryption=s3.BucketEncryption.S3_MANAGED,
        #    public_read_access=False,
        #    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        #    removal_policy=RemovalPolicy.DESTROY,
        #    auto_delete_objects=True,
        #)

        # event rule to send athena events
        #glue_table_events = events.Rule(
        #    self,
        #    "glue_table_events",
        #    description="Glue table updates to openlineage",
        #    targets=[targets.LambdaFunction(athena_lineage_lambda)],
        #    event_pattern={
        #        "source": ["aws.glue"],
        #        "detail_type": ["Glue Data Catalog Table State Change"],
        #    },
        #)