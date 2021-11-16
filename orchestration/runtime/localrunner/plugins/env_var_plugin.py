from airflow.plugins_manager import AirflowPlugin
import os
import sys

# set env vars
os.environ[
    "AIRFLOW__LINEAGE__BACKEND"
] = "openlineage.lineage_backend.OpenLineageBackend"
os.environ["AIRFLOW_VAR_OPENLINEAGE_URL"] = "http://ec2-107-21-174-143.compute-1.amazonaws.com:5000"
os.environ["AIRFLOW_VAR_OPENLINEAGE_NAMESPACE"] = "cdkdl-dev"

# set connections
os.environ[
    "AIRFLOW_CONN_REDSHIFT_CONNECTOR"
] = "postgres://cdkdl_user:6YvXgZo3YYrOMYE0AZ9KD9myoANfaOo0@redshiftcluster-0eipe8tlxv32.cswv0g7cq8og.us-east-1.redshift.amazonaws.com:5439/dev"


class EnvVarPlugin(AirflowPlugin):
    name = "env_var_plugin"