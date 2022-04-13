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
npx cdk deploy cdkdl-dev/storage/s3
```

Copy the new york taxi data into the s3 bucket using the stack output <nytaxicopy> 

-----

## Governance - lineage

Deploy marquez into a docker container running on EC2 as a UI for openlineage

```bash
# deploy the lineage stack
npx cdk deploy cdkdl-dev/governance/lineage
```

Follow up actions

1. Connect to the openlineage UX at: Outputs > <LineageUI>
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
npx cdk deploy cdkdl-dev/batch/glue
```

After deploying the stack we need to update the openlineage security group to the glue connection security group.  

1. Navigate to EC2 Security Groups: https://console.aws.amazon.com/ec2/v2/home#SecurityGroups
2. Security Group Description: OpenLineage instance sg
3. Select: Inbound Rules
4. Select: Edit Inbound Rules
5. Select: Add rule
6. Input:
    1. Type: Custom TCP
    2. Port Range: 5000
    3. Source: Glue connection sg
    4. Description: Glue connection to lineage 
5. Select: Add rule
6. Input:
    1. Type: Custom TCP
    2. Port Range: 5000
    3. Source: Glue lineage lambda sg
    4. Description: Glue Lambda to lineage 
7. Select: Save rules

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

## Orchestration - Amazon Managed Workflows for Apache AirFlow

1. Build the Amazon MWAA S3 bucket
1. Build the Amazon MWAA Env

```bash
# zip the plugins (if required)
cd ./orchestration/runtime/mwaa/plugins; zip -r ../plugins.zip ./; cd ../../../../
cd ./orchestration/runtime/mwaa/plugins; ls; cd ../../../../
# deploy mwaa
npx cdk deploy cdkdl-dev/orchestration/mwaa
# check the requirements loaded correctly ...
# check the plugins loaded correctly
```

1. Create the network path between MWAA and Openlineage
1. Navigate to EC2 Security Groups: https://console.aws.amazon.com/ec2/v2/home#SecurityGroups
2. Security Group Description: OpenLineage instance sg
3. Select: Inbound Rules
4. Select: Edit Inbound Rules
5. Select: Add rule
6. Input:
    1. Type: Custom TCP
    2. Port Range: 5000
    3. Source: MWAA sg
    4. Description: MWAA connection to lineage 
7. Select: Save rules
8. Open MWAA UX: <MWAAWebserverUrl>
9. Execute the "hello_world" DAG
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
