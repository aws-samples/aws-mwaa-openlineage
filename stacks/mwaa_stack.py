# import modules
from aws_cdk import (
    core,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_iam as iam,
    aws_logs as logs,
    aws_mwaa as mwaa,
)
from pathlib import Path
from bucket_cleaner.custom_resource import BucketCleaner

# set path
dirname = Path(__file__).parent


class MwaaStack(core.Stack):
    """
    create s3 buckets for mwaa
    create mwaa env
    """

    def __init__(
        self, scope: core.Construct, id: str, constants: dict, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # create s3 bucket for mwaa
        s3_bucket_mwaa = s3.Bucket(
            self,
            "s3_bucket_mwaa",
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        # tag the bucket
        core.Tags.of(s3_bucket_mwaa).add("project", constants["PROJECT_TAG"])
        core.Tags.of(s3_bucket_mwaa).add("purpose", "MWAA")

        # deploy requirements.txt to mwaa bucket
        s3_deploy.BucketDeployment(
            self,
            "deploy_requirements",
            destination_bucket=s3_bucket_mwaa,
            sources=[
                s3_deploy.Source.asset("./scripts/dag_code/"),
            ],
        )
        
        # role for mwaa
        airflow_role = iam.Role(
            self,
            "airflow_role",
            assumed_by=iam.ServicePrincipal("airflow-env.amazonaws.com"),
            inline_policies=[
                iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["airflow:PublishMetrics"],
                            resources=[
                                f'arn:aws:airflow:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:environment/{constants["PROJECT_TAG"]}'
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.DENY,
                            actions=["s3:ListAllMyBuckets"],
                            resources=[
                                s3_bucket_mwaa.bucket_arn,
                                f"{s3_bucket_mwaa.bucket_arn}/*",
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:List*",
                                "s3:GetBucket",
                                "s3:GetBucketPublicAccessBlock",
                            ],
                            resources=[
                                s3_bucket_mwaa.bucket_arn,
                                f"{s3_bucket_mwaa.bucket_arn}/*",
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "logs:CreateLogStream",
                                "logs:CreateLogGroup",
                                "logs:PutLogEvents",
                                "logs:GetLogEvents",
                                "logs:GetLogRecord",
                                "logs:GetLogGroupFields",
                                "logs:GetQueryResults",
                                "logs:DescribeLogGroups",
                            ],
                            resources=[
                                f"arn:aws:logs:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:log-group:{constants['PROJECT_TAG']}-*"
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["cloudwatch:PutMetricData"],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "sqs:ChangeMessageVisibility",
                                "sqs:DeleteMessage",
                                "sqs:GetQueueAttributes",
                                "sqs:GetQueueUrl",
                                "sqs:ReceiveMessage",
                                "sqs:SendMessage",
                            ],
                            resources=[
                                f"arn:aws:sqs{core.Aws.REGION}:*:airflow-celery-*"
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "kms:Decrypt",
                                "kms:DescribeKey",
                                "kms:GenerateDataKey*",
                                "kms:Encrypt",
                            ],
                            not_resources=[
                                f"arn:aws:kms:*:{core.Aws.ACCOUNT_ID}:key/*"
                            ],
                            conditions={
                                "StringLike": {
                                    "kms:ViaService": [
                                        f"sqs.{core.Aws.REGION}.amazonaws.com"
                                    ]
                                }
                            },
                        ),
                    ]
                )
            ],
        )

        # mwaa security group
        airflow_sg = ec2.SecurityGroup(self, "airflow_sg", vpc=constants["vpc"])
        # add access within group
        airflow_sg.connections.allow_from(
            airflow_sg,
            ec2.Port.all_traffic(),
            "within mwaa",
        )

        # add an airflow environment
        airflow = mwaa.CfnEnvironment(
            self,
            "airflow",
            name=constants["PROJECT_TAG"],
            dag_s3_path="dags",
            # requirements_s3_path="requirements.txt",
            # plugins_s3_path="plugins.zip",
            source_bucket_arn=s3_bucket_mwaa.bucket_arn,
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                security_group_ids=[airflow_sg.security_group_id],
                subnet_ids=constants["vpc"].select_subnets(
                    subnet_type=ec2.SubnetType.PRIVATE,
                ).subnet_ids[:2],
            ),
            execution_role_arn=airflow_role.role_arn,
            max_workers=10,
            webserver_access_mode="PUBLIC_ONLY",
        )
        # tag the mwaa
        core.Tags.of(airflow).add("project", constants["PROJECT_TAG"])

        # cleaner action on delete
        #s3_bucket_cleaner = BucketCleaner(
        #    self,
        #    "s3_bucket_cleaner",
        #    buckets=[
        #        s3_bucket_mwaa],
        #    lambda_description=f"On delete empty {core.Stack.stack_name} S3 buckets",
        #)
        #s3_bucket_cleaner.node.add_dependency(s3_bucket_mwaa)

        # set output props
        self.output_props = {}
        self.output_props["s3_bucket_mwaa"] = s3_bucket_mwaa

    # properties
    @property
    def outputs(self):
        return self.output_props
