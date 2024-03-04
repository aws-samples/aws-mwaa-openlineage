from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable, Connection
import os
import sys

openlineage_url = Variable.get('OPENLINEAGE_URL', default_var='')
os.environ["OPENLINEAGE_URL"] = openlineage_url
openlineage_namespace = Variable.get('OPENLINEAGE_NAMESPACE', default_var='')
os.environ["OPENLINEAGE_NAMESPACE"] = openlineage_namespace
os.environ["AIRFLOW_CONN_REDSHIFT_CONNECTOR"] = Connection.get_connection_from_secrets("REDSHIFT_CONNECTOR").get_uri()
# is being loaded not to path
sys.path.append('/usr/local/airflow/.local/bin')


class EnvVarPlugin(AirflowPlugin):
    name = "env_var_plugin"
