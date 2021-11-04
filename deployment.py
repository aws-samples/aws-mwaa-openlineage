from typing import Any
from aws_cdk import core as cdk

# get constants
import constants

# get stacks
from foundation.infrastructure import Storage
from governance.infrastructure import LakeFormation, Lineage
from orchestration.infrastructure import MWAA
from query.infrastructure import Athena, Redshift
from batch.infrastructure import EMR


class CDKDataLake(cdk.Stage):
    def __init__(
        self,
        scope: cdk.Construct,
        id_: str,
        **kwargs: Any,
    ):
        super().__init__(scope, id_, **kwargs)

        # foundation capabilities
        foundation = cdk.Stack(self, "foundation")
        storage = Storage(foundation, "storage")

        # governance
        # open lineage
        governance = cdk.Stack(self, "governance")
        lineage = Lineage(
            governance,
            "lineage",
            VPC=storage.VPC,
            EXTERNAL_IP=storage.EXTERNAL_IP,
            MARQUEZ_INSTANCE=constants.DEV_MARQUEZ_INSTANCE,
            KEY_PAIR=constants.DEV_KEY_PAIR,
        )
        # lake formation
        # lateformation = LakeFormation(
        #    governance,
        #    "lakeformation",
        #    VPC=storage.VPC,
        #    LF_ADMIN_USER=f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:user/{constants.DEV_LF_ADMIN_USER}",
        # )

        # orchestration
        orchestration = cdk.Stack(self, "orchestration")
        mwaa = MWAA(
            orchestration,
            "mwaa",
            VPC=storage.VPC,
            MWAA_ENV_NAME=f"{constants.CDK_APP_NAME}-dev",
            MWAA_ENV_CLASS=constants.DEV_MWAA_ENV_CLASS,
            MWAA_PLUGINS_VERSION=constants.DEV_MWAA_PLUGINS_VERSION,
            MWAA_REQUIREMENTS_VERSION=constants.DEV_MWAA_REQUIREMENTS_VERSION,
            OPENLINEAGE_URL=f"http://{lineage.MARQUEZ_URL}:5000",
            OPENLINEAGE_INSTANCE_SG=lineage.MARQUEZ_SG,
        )

        # batch
        batch = cdk.Stack(self, "batch")
        emr = EMR(
            batch,
            "emr",
            VPC=storage.VPC,
            EMR_NAME=f"{constants.CDK_APP_NAME}-dev",
            EMR_RELEASE_LABEL=constants.DEV_EMR_RELEASE_LABEL,
            EMR_CORE_INSTANCE_COUNT=constants.DEV_EMR_CORE_INSTANCE_COUNT,
            EMR_CORE_INSTANCE_TYPE=constants.DEV_EMR_CORE_INSTANCE_TYPE,
            EMR_MASTER_INSTANCE_COUNT=constants.DEV_EMR_MASTER_INSTANCE_COUNT,
            EMR_MASTER_INSTANCE_TYPE=constants.DEV_EMR_MASTER_INSTANCE_TYPE,
        )

        # query
        query = cdk.Stack(self, "query")
        athena = Athena(
            query,
            "athena",
            VPC=storage.VPC,
            ATHENA_CATALOG_NAME=f"{constants.CDK_APP_NAME}-dev",
        )
        redshift = Redshift(
            query,
            "redshift",
            VPC=storage.VPC,
            REDSHIFT_DB_NAME=f"{constants.CDK_APP_NAME}-dev",
            REDSHIFT_NUM_NODES=constants.DEV_REDSHIFT_NUM_NODES,
            REDSHIFT_NODE_TYPE=constants.DEV_REDSHIFT_NODE_TYPE,
            REDSHIFT_CLUSTER_TYPE=constants.DEV_REDSHIFT_CLUSTER_TYPE,
            REDSHIFT_MASTER_USERNAME=constants.DEV_REDSHIFT_MASTER_USERNAME,
        )
