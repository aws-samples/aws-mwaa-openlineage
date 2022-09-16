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
    aws_redshiftserverless as rs,
    aws_secretsmanager as sm,
    Aws,
    CfnOutput,
    RemovalPolicy,
    Stack,
    SecretValue,
)


class Redshift(Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        S3_BUCKET_RAW: s3.Bucket,
        REDSHIFT_DB_NAME: str,
        REDSHIFT_NAMESPACE: str,
        REDSHIFT_WORKGROUP: str,
        REDSHIFT_NUM_NODES: str,
        REDSHIFT_NODE_TYPE: str,
        REDSHIFT_CLUSTER_TYPE: str,
        REDSHIFT_MASTER_USERNAME: str,
        REDSHIFT_SG: ec2.SecurityGroup,
        VPC=ec2.Vpc,
        **kwargs
    ):
        super().__init__(scope, id, **kwargs)

        redshift_password = sm.Secret(
            self,
            "redshift_password",
            description="Redshift password",
            secret_name="REDSHIFT_PASSWORD",
            generate_secret_string=sm.SecretStringGenerator(exclude_punctuation=True),
            removal_policy=RemovalPolicy.DESTROY,
        )

        redshift_cluster_role = iam.Role(
            self,
            "redshift_cluster_role",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftAllCommandsFullAccess")
            ],
        )

        S3_BUCKET_RAW.grant_read_write(redshift_cluster_role)

        rs_namespace = rs.CfnNamespace(
            self,
            "redshiftServerlessNamespace",
            namespace_name=REDSHIFT_NAMESPACE,
            db_name=REDSHIFT_DB_NAME,
            default_iam_role_arn=redshift_cluster_role.role_arn,
            iam_roles=[redshift_cluster_role.role_arn],
            admin_username=REDSHIFT_MASTER_USERNAME,
            admin_user_password=redshift_password.secret_value.unsafe_unwrap(),
        )

        rs_workgroup = rs.CfnWorkgroup(
            self,
            "redshiftServerlessWorkgroup",
            workgroup_name=REDSHIFT_WORKGROUP,
            base_capacity=32,
            namespace_name=rs_namespace.ref,
            security_group_ids=[REDSHIFT_SG.security_group_id],
            subnet_ids=VPC.select_subnets(
                subnet_type=ec2.SubnetType.PUBLIC
            ).subnet_ids,
        )

        # redshift_cluster_subnet_group = redshift.CfnClusterSubnetGroup(
        #     self,
        #     "redshiftDemoClusterSubnetGroup",
        #     subnet_ids=VPC.select_subnets(
        #         subnet_type=ec2.SubnetType.PRIVATE_WITH_NAT
        #     ).subnet_ids,
        #     description="Redshift Demo Cluster Subnet Group",
        # )

        # redshift_cluster = redshift.CfnCluster(
        #     self,
        #     "redshift_cluster",
        #     db_name=REDSHIFT_DB_NAME,
        #     cluster_identifier='rs',
        #     cluster_type=REDSHIFT_CLUSTER_TYPE,
        #     number_of_nodes=REDSHIFT_NUM_NODES,
        #     master_username=REDSHIFT_MASTER_USERNAME,
        #     master_user_password=redshift_password.secret_value.to_string(),
        #     iam_roles=[redshift_cluster_role.role_arn],
        #     node_type=REDSHIFT_NODE_TYPE,
        #     cluster_subnet_group_name=redshift_cluster_subnet_group.ref,
        #     vpc_security_group_ids=[REDSHIFT_SG.security_group_id],
        # )

        secret_bucket_var = sm.Secret(
            self,
            "bucket_var",
            description="Bucket for raw data",
            secret_name="airflow/variables/S3_BUCKET_RAW",
            secret_string_value=SecretValue.unsafe_plain_text(f"{S3_BUCKET_RAW.bucket_name}"),
            removal_policy=RemovalPolicy.DESTROY,
        )

        secret_redshift_conn = sm.Secret(
            self,
            "secret_redshift_conn",
            description="Connector for Redshift",
            secret_name="airflow/connections/REDSHIFT_CONNECTOR",
            secret_string_value=SecretValue.unsafe_plain_text(
                # f"postgres://{REDSHIFT_MASTER_USERNAME}:{redshift_password.secret_value.to_string()}"
                # f"@{redshift_cluster.attr_endpoint_address}:{redshift_cluster.attr_endpoint_port}"
                # f"/{redshift_cluster.db_name}"
                f"postgres://{REDSHIFT_MASTER_USERNAME}:{redshift_password.secret_value.to_string()}"
                f"@{REDSHIFT_WORKGROUP}.{Aws.ACCOUNT_ID}.{Aws.REGION}.redshift-serverless.amazonaws.com:5439"
                f"/{rs_namespace.db_name}"
                ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # outputs
        # output_1 = CfnOutput(
        #     self,
        #     "RedshiftServerlessEndpoint",
        #     value=f"{REDSHIFT_WORKGROUP}.{Aws.ACCOUNT_ID}.{Aws.REGION}.redshift-serverless.amazonaws.com",
        #     description=f"RedshiftServerlessEndpoint",
        # )
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
        
        # output_4 = CfnOutput(
        #     self,
        #     "RedshiftConnectionString",
        #     value=(
        #         f"postgres://{REDSHIFT_MASTER_USERNAME}:<Password>"
        #         f"@{REDSHIFT_WORKGROUP}.{Aws.ACCOUNT_ID}.{Aws.REGION}.redshift-serverless.amazonaws.com:5439"
        #         f"/{rs_namespace.db_name}"
        #     ),
        #     description=f"Redshift Connection String",
        # )
