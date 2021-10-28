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
# create the virtual environment
python -m venv .env
# download requirements
# requirements definitions are in setup,py install_requires
.env/bin/python -m pip install -r requirements.txt
# activate the virtual environment
source .env/bin/activate
```

-----
### Bootstrap the CDK

Create the CDK configuration by bootstrapping the CDK.

```bash
# bootstrap the cdk
(.env)$ cdk bootstrap aws://youraccount/yourregion
```

-----
## Amazon Virtual Private Cloud

Use the AWS CDK to deploy an Amazon VPC across multiple availability zones. If using an existing VPC then add the VPCID into the cdk.json file.

```bash
# deploy the storage stack
cdk deploy cdkdl-dev/foundation/storage
```

1. create the vpc (or use the existing vpc)
1. create an s3 vpc endpoint for the vpc
1. create an athena vpc endpoint for the vpc **
1. create s3 bucket for scripts
1. create s3 bucket for raw data
1. create s3 bucket for processed data
1. create s3 bucket for athena results
1. create s3 bucket for logs
1. create cloudtrail for s3 bucket logging
1. deploy file from scripts directory into the raw bucket

-----
## Marquez (open lineage)

Deploy marquez into a docker container running on EC2 as a UI for lineage
* docker-compose is not recognized by the CloudFormationInit which is stopping full automation here ...

```bash
# deploy the lineage stack
cdk deploy cdkdl-dev/governance/lineage
# connect to the marquez instance
# ssh path is an output of the stack
ssh -i "your_key_pair.pem" marquez_instance_public_dns
# start marquez on the ec2 instance
cd marquez
./docker/up.sh
```

-----
## Amazon Managed Workflows for Apache AirFlow

1. Build the Amazon MWAA S3 bucket
1. Build the Amazon MWAA Env

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
1. etc.
2. Split storage into its own stack, make a deployment a domain (vpc level)?
3. add lf endpoint when it is released :/

https://f11bff86-0f66-47b3-be91-4401f58aab47.c13.us-east-1.airflow.amazonaws.com