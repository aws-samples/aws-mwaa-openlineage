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
    aws_secretsmanager as _sm,
    Aws,
    CfnOutput,
    RemovalPolicy,
    Stack
)


class Redshift(Stack):
    """create crawlers for the s3 buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        REDSHIFT_DB_NAME: str,
        REDSHIFT_NAMESPACE: str,
        REDSHIFT_WORKGROUP: str,
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

        iam.ManagedPolicy(
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

        # outputs
        output_1 = CfnOutput(
            self,
            "RedshiftServerlessEndpoint",
            value=f"{REDSHIFT_WORKGROUP}.{Aws.ACCOUNT_ID}.{Aws.REGION}.redshift-serverless.amazonaws.com",
            description=f"RedshiftServerlessEndpoint",
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
                f"@{REDSHIFT_WORKGROUP}.{Aws.ACCOUNT_ID}.{Aws.REGION}.redshift-serverless.amazonaws.com:5439"
                f"/{rs_namespace.db_name} "
            ),
            description=f"Redshift Connection String",
        )
