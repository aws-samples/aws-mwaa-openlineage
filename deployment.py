from typing import Any
from aws_cdk import core as cdk

# get constants
import constants

# get stacks
from foundation.infrastructure import Storage
from governance.infrastructure import Lineage
from orchestration.infrastructure import MWAA


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
        governance = cdk.Stack(self, "governance")
        lineage = Lineage(
            governance,
            "lineage",
            VPC=storage.VPC,
            EXTERNAL_IP=storage.EXTERNAL_IP,
            MARQUEZ_INSTANCE=constants.DEV_MARQUEZ_INSTANCE,
            KEY_PAIR=constants.DEV_KEY_PAIR,
        )

        # orchestration
        orchestration = cdk.Stack(self, "orchestration")
        mwaa = MWAA(
            orchestration,
            "mwaa",
            VPC=storage.VPC,
            MWAA_ENV_NAME=f"{constants.CDK_APP_NAME}-dev",
        )

        # governance
        # governance = cdk.Stack(self, "Governance")
        # marquez = Marquez(governance, "Marquez")
        # database = Database(
        #    stateful, "Database", dynamodb_billing_mode=database_dynamodb_billing_mode
        # )
        # stateless = cdk.Stack(self, "Stateless")
        # api = API(
        #   stateless,
        #    "API",
        #    dynamodb_table=database.table,
        #    lambda_reserved_concurrency=api_lambda_reserved_concurrency,
        # )
        # Monitoring(stateless, "Monitoring", database=database, api=api)

        # self.api_endpoint_url = api.endpoint_url
