from airflow.plugins_manager import AirflowPlugin
import os
import sys

os.environ[
    "AIRFLOW__LINEAGE__BACKEND"
] = "openlineage.lineage_backend.OpenLineageBackend"
os.environ["OPENLINEAGE_URL"] = "http://ec2-107-21-174-143.compute-1.amazonaws.com:5000"
os.environ["OPENLINEAGE_NAMESPACE"] = "cdkdl-dev"

# is being loaded not to path
sys.path.append('/usr/local/airflow/.local/bin')


class EnvVarPlugin(AirflowPlugin):
    name = "env_var_plugin"
