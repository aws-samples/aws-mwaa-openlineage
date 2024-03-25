from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable, Connection
import os
import sys

# Environment Variables for OpenLineage integration
# https://airflow.apache.org/docs/apache-airflow-providers-openlineage/1.4.0/configurations-ref.html

openlineage_transport = Variable.get('AIRFLOW__OPENLINEAGE__TRANSPORT', default_var='')
os.environ["AIRFLOW__OPENLINEAGE__TRANSPORT"] = openlineage_transport
openlineage_namespace = Variable.get('AIRFLOW__OPENLINEAGE__NAMESPACE', default_var='')
os.environ["AIRFLOW__OPENLINEAGE__NAMESPACE"] = openlineage_namespace

os.environ["AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS"] = ""
os.environ["AIRFLOW__OPENLINEAGE__CONFIG_PATH"]=""

os.environ["AIRFLOW_CONN_REDSHIFT_CONNECTOR"] = Connection.get_connection_from_secrets("REDSHIFT_CONNECTOR").get_uri()

# is being loaded not to path
sys.path.append('/usr/local/airflow/.local/bin')


class EnvVarPlugin(AirflowPlugin):
    name = "env_var_plugin"
