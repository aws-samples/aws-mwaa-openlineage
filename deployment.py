from typing import Any

# cdk stuff
from constructs import Construct
from aws_cdk import Stack, Stage

# get constants
import constants

# get stacks
from storage.infrastructure import S3
from governance.infrastructure import Lineage
from orchestration.infrastructure import MWAA
from consume.infrastructure import Athena, Redshift, SageMaker
from batch.infrastructure import EMR, Glue


class CDKDataLake(Stage):
    def __init__(
        self,
        scope: Construct,
        id_: str,
        **kwargs: Any,
    ):
        super().__init__(scope, id_, **kwargs)

        # storage
        storage = Stack(self, "storage")
        s3 = S3(storage, "s3", EXTERNAL_IP=constants.EXTERNAL_IP)

        # governance
        # open lineage
        governance = Stack(self, "governance")
        lineage = Lineage(
            governance,
            "lineage",
            VPC=s3.VPC,
            EXTERNAL_IP=constants.EXTERNAL_IP,
            LINEAGE_INSTANCE=constants.DEV_LINEAGE_INSTANCE,
            KEY_PAIR=constants.DEV_KEY_PAIR,
        )

        # batch
        batch = Stack(self, "batch")
        emr = EMR(
            batch,
            "emr",
            VPC=s3.VPC,
            EMR_NAME=constants.DEV_EMR_NAME,
            EMR_RELEASE_LABEL=constants.DEV_EMR_RELEASE_LABEL,
            EMR_CORE_INSTANCE_COUNT=constants.DEV_EMR_CORE_INSTANCE_COUNT,
            EMR_CORE_INSTANCE_TYPE=constants.DEV_EMR_CORE_INSTANCE_TYPE,
            EMR_MASTER_INSTANCE_COUNT=constants.DEV_EMR_MASTER_INSTANCE_COUNT,
            EMR_MASTER_INSTANCE_TYPE=constants.DEV_EMR_MASTER_INSTANCE_TYPE,
        )
        glue = Glue(
            batch,
            "glue",
            S3_BUCKET_RAW=s3.S3_BUCKET_RAW,
            S3_BUCKET_STAGE=s3.S3_BUCKET_STAGE,
            GLUE_DB_PREFIX=constants.DEV_GLUE_DB_PREFIX,
            OPENLINEAGE_API=lineage.OPENLINEAGE_API,
            OPENLINEAGE_NAMESPACE=constants.DEV_OPENLINEAGE_NAMESPACE,
            VPC=s3.VPC,
        )

        # consume
        consume = Stack(self, "consume")
        athena = Athena(
            consume,
            "athena",
            VPC=s3.VPC,
        )
        redshift = Redshift(
            consume,
            "redshift",
            VPC=s3.VPC,
            REDSHIFT_DB_NAME=constants.DEV_REDSHIFT_DB_NAME,
            REDSHIFT_NUM_NODES=constants.DEV_REDSHIFT_NUM_NODES,
            REDSHIFT_NODE_TYPE=constants.DEV_REDSHIFT_NODE_TYPE,
            REDSHIFT_CLUSTER_TYPE=constants.DEV_REDSHIFT_CLUSTER_TYPE,
            REDSHIFT_MASTER_USERNAME=constants.DEV_REDSHIFT_MASTER_USERNAME,
        )
        sagemaker = SageMaker(
            consume,
            "sagemaker",
            VPC=s3.VPC,
        )

        # orchestration
        orchestration = Stack(self, "orchestration")
        # mwaa
        mwaa = MWAA(
            orchestration,
            "mwaa",
            VPC=s3.VPC,
            MWAA_ENV_NAME=constants.DEV_MWAA_ENV_NAME,
            MWAA_ENV_CLASS=constants.DEV_MWAA_ENV_CLASS,
            MWAA_PLUGINS_VERSION=constants.DEV_MWAA_PLUGINS_VERSION,
            MWAA_REQUIREMENTS_VERSION=constants.DEV_MWAA_REQUIREMENTS_VERSION,
            MWAA_DEPLOY_FILES=True,
            MWAA_REPO_DAG_NAME=constants.DEV_MWAA_REPO_DAG_NAME,
        )
