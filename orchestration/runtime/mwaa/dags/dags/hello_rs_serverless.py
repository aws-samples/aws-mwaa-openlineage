from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(dag_id="rs", schedule_interval=None, start_date=days_ago(2), tags=['example']) as dag:
    create_fruit_query = '''
    CREATE TABLE IF NOT EXISTS fruit (fruit_id INTEGER, name VARCHAR NOT NULL, color VARCHAR NOT NULL);
    '''
    create_table_fruit = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_table',
        sql=create_fruit_query
    )

    insert_fruit_query = '''
    INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');
    '''
    task_insert_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_data',
        sql=insert_fruit_query
    )

    create_citrus_query= '''
    CREATE TABLE IF NOT EXISTS citrus (fruit_id INTEGER, name VARCHAR NOT NULL, color VARCHAR NOT NULL);
    '''
    create_table_citrus = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__task_create_table_citrus',
        sql=create_citrus_query
    )

    inser_citrus_query='''
    INSERT INTO citrus (fruit_id, name, color) SELECT * FROM fruit;
    '''
    task_insert_data_citrus = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_data_citrus',
        sql=inser_citrus_query
    )

    create_table_fruit >> task_insert_data >> create_table_citrus >> task_insert_data_citrus