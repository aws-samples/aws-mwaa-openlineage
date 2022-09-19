from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

s3bucket = Variable.get('S3_BUCKET_RAW')

with DAG(dag_id="rs_tickit", schedule_interval=None, start_date=days_ago(2), tags=['example']) as dag:

    create_ext_schema_query = f'''
    create external schema IF NOT EXISTS s3_datalake
    from data catalog
    database 'redshift_glue_db'
    iam_role default
    create external database if not exists;
    '''
    create_ext_schema = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='create_ext_schema',
        sql=create_ext_schema_query
    )

    create_event_query = '''
    CREATE TABLE IF NOT EXISTS event(
	eventid integer not null,
	venueid smallint not null,
	catid smallint not null,
	dateid smallint not null,
	eventname varchar(200),
	starttime timestamp);
    '''
    create_table_event = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_event_table',
        sql=create_event_query
    )

    insert_event_query = f'''
    INSERT INTO event 
    SELECT * FROM s3_datalake.external_event;
    '''
    task_insert_event_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_event_data',
        sql=insert_event_query
    )


    create_users_query = '''
    CREATE TABLE IF NOT EXISTS users(
	userid integer not null,
	username char(8),
	firstname varchar(30),
	lastname varchar(30),
	city varchar(30),
	state char(2),
	email varchar(100),
	phone char(14),
	likesports boolean,
	liketheatre boolean,
	likeconcerts boolean,
	likejazz boolean,
	likeclassical boolean,
	likeopera boolean,
	likerock boolean,
	likevegas boolean,
	likebroadway boolean,
	likemusicals boolean);
    '''
    create_table_users = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_users_table',
        sql=create_users_query
    )

    insert_users_query = f'''
    INSERT INTO users 
    SELECT * FROM s3_datalake.external_users;
    '''
    task_insert_users_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_users_data',
        sql=insert_users_query
    )

    create_venue_query = '''
    CREATE TABLE IF NOT EXISTS venue(
	venueid smallint not null,
	venuename varchar(100),
	venuecity varchar(30),
	venuestate char(2),
	venueseats integer);
    '''
    create_table_venue = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_venue_table',
        sql=create_venue_query
    )

    insert_venue_query = f'''
    INSERT INTO venue 
    SELECT * FROM s3_datalake.external_venue;
    '''
    task_insert_venue_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_venue_data',
        sql=insert_venue_query
    )

    create_category_query = '''
    CREATE TABLE IF NOT EXISTS category(
	catid smallint not null distkey sortkey,
	catgroup varchar(10),
	catname varchar(10),
	catdesc varchar(50));
    '''
    create_table_category = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_category_table',
        sql=create_category_query
    )

    insert_category_query = f'''
    INSERT INTO category 
    SELECT * FROM s3_datalake.external_category;
    '''
    task_insert_category_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_category_data',
        sql=insert_category_query
    )

    create_date_query = '''
    CREATE TABLE IF NOT EXISTS date(
	dateid smallint not null,
	caldate date not null,
	day character(3) not null,
	week smallint not null,
	month character(5) not null,
	qtr character(5) not null,
	year smallint not null,
	holiday boolean));
    '''
    create_table_date = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_date_table',
        sql=create_date_query
    )

    insert_date_query = f'''
    INSERT INTO date 
    SELECT * FROM s3_datalake.external_date;
    '''
    task_insert_date_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_date_data',
        sql=insert_date_query
    )

    create_listing_query = '''
    CREATE TABLE IF NOT EXISTS listing(
	listid integer not null,
	sellerid integer not null,
	eventid integer not null,
	dateid smallint not null,
	numtickets smallint not null,
	priceperticket decimal(8,2),
	totalprice decimal(8,2),
	listtime timestamp);
    '''
    create_table_listing = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_listing_table',
        sql=create_listing_query
    )

    insert_listing_query = f'''
    INSERT INTO listing 
    SELECT * FROM s3_datalake.external_listing;
    '''
    task_insert_listing_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_listing_data',
        sql=insert_listing_query
    )

    create_sales_query = '''
    CREATE TABLE IF NOT EXISTS sales(
	salesid integer not null,
	listid integer not null,
	sellerid integer not null,
	buyerid integer not null,
	eventid integer not null,
	dateid smallint not null,
	qtysold smallint not null,
	pricepaid decimal(8,2),
	commission decimal(8,2),
	saletime timestamp);
    '''
    create_table_sales = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_sales_table',
        sql=create_sales_query
    )

    insert_sales_query = f'''
    INSERT INTO sales 
    SELECT * FROM s3_datalake.external_sales;
    '''
    task_insert_sales_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_sales_data',
        sql=insert_sales_query
    )

    create_fact_sales_query = '''
    CREATE TABLE IF NOT EXISTS sales(
	salesid integer not null,
	listid integer not null,
	sellerid integer not null,
	buyerid integer not null,
	eventid integer not null,
	dateid smallint not null,
	qtysold smallint not null,
	pricepaid decimal(8,2),
	commission decimal(8,2),
	saletime timestamp);
    '''
    create_table_fact_sales = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_fact_sales_table',
        sql=create_fact_sales_query
    )

    insert_fact_sales_query = f'''
    INSERT INTO sales 
    SELECT * FROM s3_datalake.external_sales;
    '''
    task_insert_fact_sales_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_fact_sales_data',
        sql=insert_fact_sales_query
    )

    create_dim_event_query = '''
    CREATE TABLE IF NOT EXISTS sales(
	salesid integer not null,
	listid integer not null,
	sellerid integer not null,
	buyerid integer not null,
	eventid integer not null,
	dateid smallint not null,
	qtysold smallint not null,
	pricepaid decimal(8,2),
	commission decimal(8,2),
	saletime timestamp);
    '''
    create_table_dim_event = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_dim_event_table',
        sql=create_dim_event_query
    )

    insert_dim_event_query = f'''
    INSERT INTO sales 
    SELECT * FROM s3_datalake.external_sales;
    '''
    task_insert_dim_event_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_dim_event_data',
        sql=insert_dim_event_query
    )

    create_dim_users_query = '''
    CREATE TABLE IF NOT EXISTS sales(
	salesid integer not null,
	listid integer not null,
	sellerid integer not null,
	buyerid integer not null,
	eventid integer not null,
	dateid smallint not null,
	qtysold smallint not null,
	pricepaid decimal(8,2),
	commission decimal(8,2),
	saletime timestamp);
    '''
    create_table_dim_users = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='setup__create_dim_users_table',
        sql=create_dim_users_query
    )

    insert_dim_users_query = f'''
    INSERT INTO sales 
    SELECT * FROM s3_datalake.external_sales;
    '''
    task_insert_dim_users_data = PostgresOperator(
        postgres_conn_id='REDSHIFT_CONNECTOR',
        task_id='task_insert_dim_users_data',
        sql=insert_dim_users_query
    )


    create_ext_schema >> create_table_event >> task_insert_event_data
    create_ext_schema >> create_table_users >> task_insert_users_data
    create_ext_schema >> create_table_venue >> task_insert_venue_data
    create_ext_schema >> create_table_category >> task_insert_category_data
    create_ext_schema >> create_table_date >> task_insert_date_data
    create_ext_schema >> create_table_listing >> task_insert_listing_data
    create_ext_schema >> create_table_sales >> task_insert_sales_data
    [task_insert_date_data, task_insert_sales_data, task_insert_listing_data] >> create_table_fact_sales >> task_insert_fact_sales_data
    [task_insert_category_data, task_insert_venue_data, task_insert_event_data] >> create_table_dim_event >> task_insert_dim_event_data
    task_insert_users_data >> create_table_dim_users >> task_insert_dim_users_data