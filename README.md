# AWS CDK - Datalake

Use the AWS CDK to create a data lake.

-----
### Set up the environment

Create a Cloud9 Environment

Navigate to: https://us-east-1.console.aws.amazon.com/cloud9/home/create

Name: openlineage_cloud9

Instance type: t3.small

At Cloud9 terminal session:

```bash
# Clone Git repository
git clone https://gitlab.aws.dev/analytics-apj/anz/aws-cdk-datalake.git
# Create a personal access token in gitlab: https://gitlab.aws.dev/-/profile/personal_access_tokens
# Username for 'https://gitlab.aws.dev/analytics-apj/anz/aws-cdk-datalake.git': 
pat
# Password for 'https://pat@gitlab.aws.dev/analytics-apj/anz/aws-cdk-datalake.git':
<Enter personal access token>
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
# confirm CDK version. Cloud9 comes preinstalled with CDK version 2.33.0
cdk --version
# bookstrap if required
cdk bootstrap
```

Note: When using cloud9 you have the option to enable auto-save. Go to Cloud9 menu &rarr; Preferences (&#8984; , ) &rarr; Experimental tab &rarr; Auto-Save Files: After Delay

Open constants.py

Update DEV_KEY_PAIR with an existing key pair or create a new one through the console: https://us-east-1.console.aws.amazon.com/ec2/v2/home?region=us-east-1#CreateKeyPair 

Update DEV_LF_ADMIN_USER. https://docs.aws.amazon.com/lake-formation/latest/dg/getting-started-setup.html

Update EXTERNAL_IP with the local IP address.

```python
# Update Key Pair
DEV_KEY_PAIR = "key_openlineage"

# Update LakeFormation Admin
DEV_LF_ADMIN_USER = "LFAdmin" 

# Update client external ip. From your local, browse to https://ident.me
EXTERNAL_IP = "<Local IP address>"
```

## Foundation - Amazon Virtual Private Cloud

Use the AWS CDK to deploy an Amazon VPC across multiple availability zones.

```bash
# deploy the storage stack
cdk deploy cdkdl-dev/storage/s3
```

Copy the new york taxi data into the s3 bucket using the stack output <nytaxicopy> 

-----

## Governance - lineage

Deploy marquez into a docker container running on EC2 as a UI for openlineage

```bash
# deploy the lineage stack
cdk deploy cdkdl-dev/governance/lineage
```

Follow up actions

1. Connect to the openlineage UX. URL found at CDK or Cloudformation outputs  <LineageUI>
2. Create openlineage variables in AWS Secrets Manager
3. Navigate to AWS Secrets Manager: https://console.aws.amazon.com/secretsmanager/
4. Select: Store a new secret
5. Input:
   1. Type = "Other type of secret"
   2. Plaintext = "<OpenlineageAPI>"
   3. Secret name = airflow/variables/OPENLINEAGE_URL
   4. Secret rotation = No

7. Create a secret in AWS Secrets Manager:
   1. Type = "Other type of secret"
   2. Plaintext = "cdkdl-dev"
   3. Secret Name = airflow/variables/OPENLINEAGE_NAMESPACE
   4. Secret rotation = No

-----

## Batch - Glue catalog

Create a glue database and glue crawler

```bash
# deploy the glue stack
cdk deploy cdkdl-dev/batch/glue
```

-----

## Query - Amazon Redshift

1. Build the Redshift cluster
2. Create the cluster password in secrets manager

```bash
# deploy the redshift stack
cdk deploy cdkdl-dev/consume/redshift
```

Follow up actions:
1. Get the Redshift connection string from CDK Output.
2. Update the password in the connection string. The Redshift password can be found in AWS Secrets Manager with secret name: "REDSHIFT_PASSWORD" 
3. Create a secret in AWS Secrets Manager:
   1. key = "airflow/connections/REDSHIFT_CONNECTOR"
   1. value = "Redshift connection_string"
-----

## Orchestration - Amazon Managed Workflows for Apache AirFlow

1. Build the Amazon MWAA S3 bucket
1. Build the Amazon MWAA Env

```bash
# zip the plugins (if required)
cd ./orchestration/runtime/mwaa/plugins; zip -r ../plugins.zip ./; cd ../../../../
cd ./orchestration/runtime/mwaa/plugins; ls; cd ../../../../
# deploy mwaa
cdk deploy cdkdl-dev/orchestration/mwaa
# check the requirements loaded correctly ...
# check the plugins loaded correctly
```

1. Open MWAA UX: <MWAAWebserverUrl>
2. Execute the "hello_world" DAG
    1. Confirm the execution details appear in the openlineage UX

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
