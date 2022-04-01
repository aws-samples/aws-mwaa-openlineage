# AWS CDK - Datalake
 
Use the AWS CDK to create a data lake.

-----
### Set up the environment

At a bash terminal session.

```bash
# install the cdkv2 if required
npm install -g aws-cdk@next
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
# bookstrap if required
npx cdk bootstrap
```
-----

## Foundation - Amazon Virtual Private Cloud

Use the AWS CDK to deploy an Amazon VPC across multiple availability zones.
The foundation stack also 

```bash
# deploy the storage stack
npx cdk deploy cdkdl-dev/foundation/storage
```

Copy the new york taxi data into the s3 bucket using the stack output <nytaxicopy> 

-----

## Governance - lineage

Deploy marquez into a docker container running on EC2 as a UI for openlineage

```bash
# deploy the lineage stack
npx cdk deploy cdkdl-dev/governance/lineage
```

Follow up actions:
1. Connect to the openlineage UX at: Outputs > <LineageUI>
2. Create a secret (type="Other type of secret") in AWS Secrets Manager:
   1. Type = "Other type of secret"
   2. Plaintext = "openlineage.lineage_backend.OpenLineageBackend"
   2. Secret name = airflow/variables/AIRFLOW__LINEAGE__BACKEND
   4. Secret rotation = No
3. Create a secret in AWS Secrets Manager:
   1. Type = "Other type of secret"
   2. Plaintext = "<OpenlineageAPI>"
   3. Secret name = airflow/variables/OPENLINEAGE_URL
   4. Secret rotation = No

4. Create a secret in AWS Secrets Manager:
   1. Type = "Other type of secret"
   2. Plaintext = "cdkdl-dev"
   3. Secret Name = airflow/variables/OPENLINEAGE_NAMESPACE
   4. Secret rotation = No

-----

## Batch - Glue catalog

Create a glue database and glue crawler

```bash
# deploy the glue stack
npx cdk deploy cdkdl-dev/batch/glue
```

After deploying the stack we need to update the openlineage security group to the glue connection security group.  

Security Group: openlineage security group
Type: Custom TCP
Port Range: 5000
Source: Glue connection sg
Description: glue connection to lineage 

Security Group: openlineage security group
Type: Custom TCP
Port Range: 5000
Source: Glue lineage lambda sg
Description: glue lambda to lineage 

Create the Spark UI
```bash
# build the image
cd batch/runtime/sparkui
docker build -t glue/sparkui:latest . 
# run the server
# set log dir to output path s3-bucket-spark-logs
LOG_DIR="s3a://cdkdl-dev-batchgluebb9040d3-s3bucketsparkee88c9bc-o56kii9tg57f/logs/"
# assumes using default profile
PROFILE_NAME="default"
docker run -itd -v ~/.aws:/root/.aws -e AWS_PROFILE=$PROFILE_NAME -e SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS -Dspark.history.fs.logDirectory=$LOG_DIR  -Dspark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" -p 18080:18080 glue/sparkui:latest "/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer"
```

View the Spark UI using Docker
Open http://localhost:18080 in your browser


-----

## Query - Amazon Redshift

1. Build the redshift cluster
2. Create the cluster password in secrets manager

```bash
# deploy the redshift stack
npx cdk deploy cdkdl-dev/query/redshift
```

Follow up actions:
1. Build the MWAA Redshift connection string as:
```python
    login = <mylogin>
    password = <mypassword>
    host = <redshifthost>
    port =5439
    schema = "dev"
    print(f"postgres://{login}:{password}@{host}:{port}/{schema}")
```
   1. RedshiftMasterPassword can be found in AWS Secrets Manager us "REDSHIFT_PASSWORD" 
3. Create a secret in AWS Secrets Manager:
    1. key = "airflow/connections/REDSHIFT_CONNECTOR"
    2. value = <connection_string"

-----

## Orchestration - MWAA local runner 

1. Build an instance to for MWAA local runner 

```bash
npx cdk deploy cdkdl-dev/orchestration/localrunner
```

Install the local runner ...

```bash
git clone https://github.com/aws/aws-mwaa-local-runner.git
cd aws-mwaa-local-runner
# build the image
./mwaa-local-env build-image
```

To dev files for dev:
Copy dag files to `aws-mwaa-local-runner/dags/*`
requirements.txt (or requirements.in) to `aws-mwaa-local-runner/dags/requirements.txt` 
Plugins to `plugins/*`

In `docker/config/airflow.cfg` set `lazy_load_plugins = False`

```bash
# test reqirements
./mwaa-local-env test-requirements
```

Start the remote airflow local runner

```bash
./mwaa-local-env start
```

Next
`
1. Enable security group ingress from mwaa local runner to lineage api
2. Confirm local runner can access lineage
3. Work out how to debug PostgresOperator


## Orchestration - Amazon Managed Workflows for Apache AirFlow

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

## Query - Amazon Athena

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
9. + Send EMR Spark job via AirFlow
11. Send Glue crawler job via AirFlow
12. Send Redshift copy job via AirFlow
13. Create S3 sensor with AirFlow, and register data via lineage?
14. Remove local runner dependency for openlineage stack

https://f11bff86-0f66-47b3-be91-4401f58aab47.c13.us-east-1.airflow.amazonaws.com
