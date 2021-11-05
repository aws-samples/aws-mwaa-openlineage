# AWS CDK - Datalake
 
Use the AWS CDK to create a data lake.

-----
### Set up the environment

At a bash terminal session.

```bash
# clone the repo
git clone git@ssh.gitlab.aws.dev:mcgregf/aws-cdk-datalake.git
# move to directory
cd aws-cdk-datalake
# create the virtual environment and install dependencies
python -m venv .env
# activate the virtual environment
source .env/bin/activate
# get pip tools
.env/bin/python -m pip install pip-tools
# run setup scripts
# builds requirements.txt from setup.py and installs requirements
# builds requirements.txt for each included requirements.in
./scripts/install-deps.sh
```

-----
## Amazon Virtual Private Cloud

Use the AWS CDK to deploy an Amazon VPC across multiple availability zones. If using an existing VPC then add the VPCID into the cdk.json file.

```bash
# deploy the storage stack
npx cdk deploy cdkdl-dev/foundation/storage
```

1. create the vpc
1. create an s3 vpc endpoint for the vpc
1. create s3 bucket for scripts
1. create s3 bucket for raw data
1. create s3 bucket for processed data
1. deploy data files from scripts directory into the raw bucket

-----
## Marquez (open lineage)

Deploy marquez into a docker container running on EC2 as a UI for lineage

```bash
# deploy the lineage stack
npx cdk deploy cdkdl-dev/governance/lineage
```

Follow up actions:
1. Connect to the openlineage UX at: Outputs > <MarquezUI>
2. Create a secret in AWS Secrets Manager:
    1. key = "airflow/variables/AIRFLOW__LINEAGE__BACKEND"
    2. value = "openlineage.lineage_backend.OpenLineageBackend"
3. Create a secret in AWS Secrets Manager:
    1. key = "airflow/variables/OPENLINEAGE_URL"
    2. value = <OpenlineageAPI>
4. Create a secret in AWS Secrets Manager:
    1. key = "airflow/variables/OPENLINEAGE_NAMESPACE"
    2. value = "cdkdl-dev"

-----
## Amazon Redshift

1. Build the redshift cluster
2. Create the cluster password in secrets manager

```bash
# deploy the redshift stack
npx cdk deploy cdkdl-dev/query/redshift
```

Follow up actions:
1. Build the MWAA Redshift connection string as:
```python
    connection_string = f"postgres://{login}:{password}@{host}:{port}/{schema}"
```
   1. RedshiftMasterPassword can be found in AWS Secrets Manager us "REDSHIFT_PASSWORD" 
3. Create a secret in AWS Secrets Manager:
    1. key = "airflow/connections/REDSHIFT_CONNECTOR"
    2. value = <connection_string"

-----
## Amazon Managed Workflows for Apache AirFlow

1. Build the Amazon MWAA S3 bucket
1. Build the Amazon MWAA Env

```bash
# Update the MWAA ENV VAR Plugin env_var_plugin.py
# SET os.environ["OPENLINEAGE_URL"] = <OPENLINEAGE_API>
# SET os.environ["OPENLINEAGE_NAMESPACE"] = <namespace>
```

```bash
# zip the plugins (if required)
zip -r ./orchestration/runtime/mwaa/plugins.zip ./orchestration/runtime/mwaa/plugins/
# deploy mwaa
npx cdk deploy cdkdl-dev/orchestration/mwaa
# check the requirements loaded correctly ...
# check the plugins loaded correctly
```

Follow up actions:
1. Open MWAA UX: <MWAAWebserverUrl>
2. Execute the "hello_world" DAG
    1. Confirm the execution details appear in the openlineage UX
2. Execute the "hello_postgres" DAG

-----
## Amazon Athena

```bash
# deploy athena stakc
npx cdk deploy cdkdl-dev/query/athena
```

-----
## AWS Lake Formation

1. Create administrators and lf roles
1. Register the s3 bucket
1. Create the databases

-----
## AWS Glue

1. Create the crawler role and crawler for the raw bucket

-----
## Next Steps
1. Create Athena view against crawled table to demonstrate FGAC
1. Create EMR job to be executed from MWAA using transient cluster to create parquet file in curated bucket
7. Create athena catalog and link to main account
8. Set Athena query location bucket with s3
9. Send EMR Spark job via AirFlow
10. Send Redshift SQL job via AirFlow
11. Send Glue crawler job via AirFlow
12. Send Redshift copy job via AirFlow
13. Create S3 sensor with AirFlow, and register data via lineage?
14. Move airflow env vars to secrets
14. Create Redshift connection into secrets

https://f11bff86-0f66-47b3-be91-4401f58aab47.c13.us-east-1.airflow.amazonaws.com
