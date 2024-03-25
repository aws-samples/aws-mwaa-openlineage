#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from datetime import datetime

from airflow import settings
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftCreateClusterSnapshotOperator,
    RedshiftDeleteClusterOperator,
    RedshiftDeleteClusterSnapshotOperator,
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
import os
# from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_redshift"


os.environ["AIRFLOW__OPENLINEAGE__TRANSPORT"]='{"type": "http", "url": "http://ec2-54-69-36-59.us-west-2.compute.amazonaws.com:5000"}'
os.environ["AIRFLOW__OPENLINEAGE__NAMESPACE"]="redshift"
os.environ["AIRFLOW__OPENLINEAGE__CONFIG_PATH"]=""
os.environ["AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS"] = ""

@task
def create_connection(conn_id_name: str, workgroup_id: str):
    # redshift_hook = RedshiftHook()
    conn = Connection(
        conn_id=conn_id_name,
        conn_type="redshift",
        host="default.349303572007.ap-southeast-2.redshift-serverless.amazonaws.com",
        login="admin",
        password="xxj(8Q-1lRF!tlLW",
        port="5439",
        schema="dev",
    )
    session = settings.Session()
    session.add(conn)
    session.commit()


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    env_id = 'dev'
    redshift_workgroup = f"default"
    conn_id_name = f"{env_id}-conn-id1"


#    set_up_connection = create_connection(conn_id_name, workgroup_id=redshift_workgroup)
    
    conn = Connection(
        conn_id=conn_id_name,
        conn_type="redshift",
        host="default.349303572007.ap-southeast-2.redshift-serverless.amazonaws.com",
        login="admin",
        password="xxj(8Q-1lRF!tlLW",
        port="5439",
        schema="dev",
    )
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=conn_id_name,
        sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
    )
    
    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id=conn_id_name,
        sql=["INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
            "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
            "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
            "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
            "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
            "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');"],
    )
    
    create_fruit_agg = SQLExecuteQueryOperator(
        task_id="create_fruit_agg",
        conn_id=conn_id_name,
        sql=["CREATE TABLE IF NOT EXISTS fruit_agg (name VARCHAR, cnt INTEGER);"],
    )
    
    insert_fruit_agg = SQLExecuteQueryOperator(
        task_id="insert_fruit_agg",
        conn_id=conn_id_name,
        sql=["INSERT  INTO fruit_agg SELECT name, count(1) cnt from fruit group by name; "],
    )


    chain(

#        set_up_connection,
        create_table,
        insert_data,
        create_fruit_agg,
        insert_fruit_agg

    )
