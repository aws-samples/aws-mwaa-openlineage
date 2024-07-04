# import modules
from constructs import Construct
from aws_cdk import (
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_lakeformation as lf,
    aws_secretsmanager as sm,
    aws_s3 as s3,
    aws_datazone as dz,
    Aws,
    CfnOutput,
    Duration,
    Stack,
    RemovalPolicy,
    SecretValue
)
from pathlib import Path

# set path
dirname = Path(__file__).parent


class DataZone(Stack):
    """
    Deploy ec2 instance
    Clone marquez
    Run marquez
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        S3_BUCKET_RAW: s3.Bucket,
        **kwargs):
        
        super().__init__(scope, id, **kwargs)
        
        
        datazoneaccessrole = iam.Role(
            self,
            "datazoneaccessrole",
            assumed_by=iam.ServicePrincipal("datazone.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonDataZoneGlueManageAccessRolePolicy"
                )
            ],
        )
        
        
        datazoneprovisioningrole = iam.Role(
            self,
            "datazoneprovisioningrole",
            assumed_by=iam.ServicePrincipal("datazone.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonDataZoneRedshiftGlueProvisioningPolicy"
                )
            ],
        )
        
        datazoneexecutionrole = iam.Role(
            self,
            "datazoneexecutionrole",
            assumed_by=iam.ServicePrincipal("datazone.amazonaws.com").with_session_tags(),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonDataZoneDomainExecutionRolePolicy"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "IAMReadOnlyAccess"
                )
            ],
        )
        
        
        dz_domain = dz.CfnDomain(self, "dz_domain",
            domain_execution_role=datazoneexecutionrole.role_arn,
            name="blog_dz_domain",
        )
        
        dz_sales_project = dz.CfnProject(
            self,
            "salesdatazoneproject",
            domain_identifier=dz_domain.attr_id,
            name="Sales producer project",
        )
        
        dz_finance_project = dz.CfnProject(
            self,
            "financedatazoneproject",
            domain_identifier=dz_domain.attr_id,
            name="Finance consumer project",
        )
        
        dz_blueprint_dwhf = dz.CfnEnvironmentBlueprintConfiguration(
            self,
            "dz_blueprint_dwhf",
            domain_identifier=dz_domain.attr_id,
            environment_blueprint_identifier="DefaultDataWarehouse",
            manage_access_role_arn=datazoneaccessrole.role_arn,
            provisioning_role_arn=datazoneprovisioningrole.role_arn,
            enabled_regions=[
                Aws.REGION
            ]
        )
        
        dz_blueprint_dl = dz.CfnEnvironmentBlueprintConfiguration(
            self,
            "dz_blueprint_dl",
            domain_identifier=dz_domain.attr_id,
            environment_blueprint_identifier="DefaultDataLake",
            manage_access_role_arn=datazoneaccessrole.role_arn,
            provisioning_role_arn=datazoneprovisioningrole.role_arn,
            regional_parameters=[dz.CfnEnvironmentBlueprintConfiguration.RegionalParameterProperty(
                parameters={
                    "S3Location": f"s3://{S3_BUCKET_RAW.bucket_name}/datazone"
                },
                region=Aws.REGION
            )],
            enabled_regions=[
                Aws.REGION
            ]

        )
        
#   salesdatazoneenvironmentprofile:
#     Type: AWS::DataZone::EnvironmentProfile
#     Properties:
#       AwsAccountId:
#         Ref: AWS::AccountId
#       AwsAccountRegion:
#         Ref: AWS::Region
#       Description: Amazon DataZone environment profile used by Sales team
#       DomainIdentifier:
#         Ref: datazonedomain
#       EnvironmentBlueprintIdentifier:
#         Fn::GetAtt:
#           - datazoneblueprintconfiguration
#           - EnvironmentBlueprintId
#       Name: sales_environment_profile
#       ProjectIdentifier:
#         Fn::GetAtt:
#           - salesdatazoneproject
#           - Id
#   financedatazoneenvironmentprofile:
#     Type: AWS::DataZone::EnvironmentProfile
#     Properties:
#       AwsAccountId:
#         Ref: AWS::AccountId
#       AwsAccountRegion:
#         Ref: AWS::Region
#       Description: Amazon DataZone environment profile used by Finance team
#       DomainIdentifier:
#         Ref: datazonedomain
#       EnvironmentBlueprintIdentifier:
#         Fn::GetAtt:
#           - datazoneblueprintconfiguration
#           - EnvironmentBlueprintId
#       Name: finance_environment_profile
#       ProjectIdentifier:
#         Fn::GetAtt:
#           - financedatazoneproject
#           - Id
#   salesdatazoneenvironment:
#     Type: AWS::DataZone::Environment
#     Properties:
#       Description: Amazon DataZone environment used by Sales team
#       DomainIdentifier:
#         Ref: datazonedomain
#       EnvironmentProfileIdentifier:
#         Fn::GetAtt:
#           - salesdatazoneenvironmentprofile
#           - Id
#       Name: sales_dz_environment
#       ProjectIdentifier:
#         Fn::GetAtt:
#           - salesdatazoneproject
#           - Id
#       UserParameters:
#         - Name: consumerGlueDbName
#           Value:
#             Fn::Join:
#               - ""
#               - - sales_consumer_db_
#                 - Ref: tickitdb
#         - Name: producerGlueDbName
#           Value:
#             Fn::Join:
#               - ""
#               - - sales_producer_db_
#                 - Ref: tickitdb
#   financedatazoneenvironment:
#     Type: AWS::DataZone::Environment
#     Properties:
#       Description: Amazon DataZone environment used by Finance team
#       DomainIdentifier:
#         Ref: datazonedomain
#       EnvironmentProfileIdentifier:
#         Fn::GetAtt:
#           - financedatazoneenvironmentprofile
#           - Id
#       Name: finance_dz_environment
#       ProjectIdentifier:
#         Fn::GetAtt:
#           - financedatazoneproject
#           - Id
#       UserParameters:
#         - Name: consumerGlueDbName
#           Value:
#             Fn::Join:
#               - ""
#               - - finance_consumer_db_
#                 - Ref: tickitdb
#         - Name: producerGlueDbName
#           Value:
#             Fn::Join:
#               - ""
#               - - finance_producer_db_
#                 - Ref: tickitdb
#   datazonedatasource:
#     Type: AWS::DataZone::DataSource
#     Properties:
#       Configuration:
#         GlueRunConfiguration:
#           RelationalFilterConfigurations:
#             - DatabaseName:
#                 Ref: tickitdb
#               FilterExpressions:
#                 - Expression: "*"
#                   Type: INCLUDE
#       Description: Tickit database sourced from AWS Glue Data Catalog
#       DomainIdentifier:
#         Ref: datazonedomain
#       EnableSetting: ENABLED
#       EnvironmentIdentifier:
#         Fn::GetAtt:
#           - salesdatazoneenvironment
#           - Id
#       Name: tickit_datasource
#       ProjectIdentifier:
#         Fn::GetAtt:
#           - salesdatazoneproject
#           - Id
#       PublishOnImport: true
#       Recommendation:
#         EnableBusinessNameGeneration: true
#       Schedule:
#         Schedule: cron(0 7 * * ? *)
#       Type: GLUE

        
        # secret_openlineage_namespace = sm.Secret(
        #     self,
        #     "openlineage_namespace",
        #     description="Openlineage Namespace",
        #     secret_name="airflow/variables/OPENLINEAGE_NAMESPACE",
        #     secret_string_value=SecretValue.unsafe_plain_text(OPENLINEAGE_NAMESPACE),
        #     removal_policy=RemovalPolicy.DESTROY,
        # )

        # secret_openlineage_url = sm.Secret(
        #     self,
        #     "openlineage_url",
        #     description="Openlineage URL",
        #     secret_name="airflow/variables/OPENLINEAGE_URL",
        #     secret_string_value=SecretValue.unsafe_plain_text(f"http://{lineage_instance.instance_public_dns_name}:5000"),
        #     removal_policy=RemovalPolicy.DESTROY,
        # )

        # # create Outputs
        # CfnOutput(
        #     self,
        #     "LineageUI",
        #     value=f"http://{lineage_instance.instance_public_dns_name}:3000",
        #     export_name="lineage-ui",
        # )
        # CfnOutput(
        #     self,
        #     "OpenlineageApi",
        #     value=f"http://{lineage_instance.instance_public_dns_name}:5000",
        #     export_name="openlineage-api",
        # )

