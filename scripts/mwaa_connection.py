import urllib.parse

conn_type = "postgres"
host = "redshiftcluster-0eipe8tlxv32.cswv0g7cq8og.us-east-1.redshift.amazonaws.com"
schema = "dev"
login = "cdkdl_user"
password = "6YvXgZo3YYrOMYE0AZ9KD9myoANfaOo0"
port = "5439"
role_arn = urllib.parse.quote_plus(
    "arn:aws:iam::820406222077:role/cdkdl-dev-orchestrationmwaa115-airflowrole209BB391-AJRDWOQ6FIOV"
)
region_name = "us-east-1"

conn_string = "{0}://{1}:{2}@{3}:{4}?role_arn={5}&region_name={6}".format(
    conn_type, login, password, host, port, role_arn, region_name
)
# print(f"{conn_type}://{host}:{port}@{login}:{password}")
# print(f"{conn_type}://{login}:{password}@{login}:{password}")

print(f"postgres://{login}:{password}@{host}:{port}/{schema}?param1=val1&param2=val2")

# postgres://cdkdl_user:2keMb1JUbezKcFIwd42DOPln6ryHUGac@redshiftcluster-0eipe8tlxv32.cswv0g7cq8og.us-east-1.redshift.amazonaws.com:5439

conn_type = "postpres"
host = "YOUR_MWAA_AIRFLOW_UI_URL"
port = "YOUR_PORT"
login = "YOUR_AWS_ACCESS_KEY_ID"
password = "YOUR_AWS_SECRET_ACCESS_KEY"
role_arn = urllib.parse.quote_plus("YOUR_EXECUTION_ROLE_ARN")
region_name = "YOUR_REGION"

conn_string = "{0}://{1}:{2}@{3}:{4}?role_arn={5}&region_name={6}".format(
    conn_type, login, password, host, port, role_arn, region_name
)
# print(conn_string)
