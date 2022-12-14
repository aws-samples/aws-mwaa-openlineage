from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

with DAG(dag_id="rs_staging_to_dm", schedule_interval=None, start_date=days_ago(2), tags=['example']) as dag:

    create_fact_sales_query = '''
    CREATE TABLE IF NOT EXISTS public.fact_sales
    (
        salesid INTEGER   
        ,listid INTEGER   
        ,sellerid INTEGER   
        ,buyerid INTEGER   
        ,eventid INTEGER   
        ,dateid SMALLINT   
        ,"day" CHAR(3)   
        ,"month" CHAR(5)   
        ,"year" SMALLINT   
        ,qtysold SMALLINT   
        ,pricepaid NUMERIC(8,2)   
        ,commission NUMERIC(8,2)   
        ,saletime TIMESTAMP WITHOUT TIME ZONE   
    );
    DELETE FROM public.fact_sales;
    '''
    create_table_fact_sales = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_fact_sales_table',
        sql=create_fact_sales_query
    )

    insert_fact_sales_query = f'''
    INSERT INTO fact_sales 
    SELECT salesid, listid, sellerid, buyerid, eventid, sl.dateid, day, month, year, qtysold, pricepaid, commission, saletime
    FROM public.sales sl
    inner join public."date" d on sl.dateid = d.dateid ;
    '''
    task_insert_fact_sales_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_fact_sales_data',
        sql=insert_fact_sales_query
    )

    create_dim_event_query = '''
    CREATE TABLE IF NOT EXISTS public.dim_event
    (
        eventid INTEGER   
        ,venueid SMALLINT   
        ,venuename VARCHAR(100)   
        ,venuecity VARCHAR(30)   
        ,venuestate CHAR(2)   
        ,venueseats INTEGER   
        ,catid SMALLINT   
        ,catgroup VARCHAR(10)   
        ,catname VARCHAR(10)   
        ,catdesc VARCHAR(50)   
        ,dateid SMALLINT   
        ,"day" CHAR(3)   
        ,"month" CHAR(5)   
        ,"year" SMALLINT   
        ,eventname VARCHAR(200)   
        ,starttime TIMESTAMP WITHOUT TIME ZONE   
    );
    DELETE FROM public.dim_event;
    '''
    create_table_dim_event = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_dim_event_table',
        sql=create_dim_event_query
    )

    insert_dim_event_query = f'''
    INSERT INTO dim_event
    SELECT eventid, ev.venueid, venuename, venuecity, venuestate, venueseats, 
    ev.catid, catgroup, catname, catdesc, ev.dateid, day, month, year, eventname, starttime
    FROM public.event ev
    inner join public."date" d on ev.dateid = d.dateid
    inner join public.category cat on ev.catid = cat.catid 
    inner join public.venue v on ev.venueid = v.venueid ;
    '''
    task_insert_dim_event_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_dim_event_data',
        sql=insert_dim_event_query
    )

    create_dim_users_query = '''
    CREATE TABLE IF NOT EXISTS public.dim_users
    (
        userid INTEGER   
        ,username CHAR(8)   
        ,firstname VARCHAR(30)   
        ,lastname VARCHAR(30)   
        ,city VARCHAR(30)   
        ,state CHAR(2)   
        ,email VARCHAR(100)   
        ,phone CHAR(14)   
        ,likesports BOOLEAN   
        ,liketheatre BOOLEAN   
        ,likeconcerts BOOLEAN   
        ,likejazz BOOLEAN   
        ,likeclassical BOOLEAN   
        ,likeopera BOOLEAN   
        ,likerock BOOLEAN   
        ,likevegas BOOLEAN   
        ,likebroadway BOOLEAN   
        ,likemusicals BOOLEAN   
    )
    ;
    DELETE FROM public.dim_users;
    '''
    create_table_dim_users = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_dim_users_table',
        sql=create_dim_users_query
    )

    insert_dim_users_query = f'''
    INSERT INTO dim_users 
    SELECT userid, username, firstname, lastname, city, state, email, phone, 
    likesports, liketheatre, likeconcerts, likejazz, likeclassical, likeopera, likerock, likevegas, likebroadway, likemusicals
    FROM public.users;
    '''
    task_insert_dim_users_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_dim_users_data',
        sql=insert_dim_users_query
    )


create_table_fact_sales >> task_insert_fact_sales_data
create_table_dim_event >> task_insert_dim_event_data
create_table_dim_users >> task_insert_dim_users_data