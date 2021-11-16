import datetime

# from airflow import DAG
from openlineage.airflow.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# set default args
args = {"postgres_conn_id": "REDSHIFT_CONNECTOR"}

with DAG(
    dag_id="hello_postgres",
    default_args=args,
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id BIGINT IDENTITY(0,1),
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )
    get_all_pets = PostgresOperator(
        task_id="get_all_pets",
        sql="SELECT * FROM pet;",
    )
    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        sql="""
            SELECT * FROM pet
            WHERE birth_date
            BETWEEN '{{ params.begin_date }}' AND '{{ params.end_date }}';
            """,
        params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
    )

    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
