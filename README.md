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
# deploy the vpc stack
(.env)$ cdk deploy elkk-vpc
```
1. create the vpc
1. create an s3 vpc endpoint
1. create an athena vpc endpoint
1. create s3 bucket for scripts
1. create s3 bucket for raw data
1. create s3 bucket for processed data
1. create s3 bucket for serving data
1. create s3 bucket for athena results
1. create s3 bucket for logs
1. create cloudtrail for s3 bucket logging
1. create a custom function to empty the s3 buckets on destroy
1. deploy file from scripts directory into the raw bucket

-----
## Amazon EMR

Add this for tuning?
https://aws.amazon.com/blogs/big-data/tune-hadoop-and-spark-performance-with-dr-elephant-and-sparklens-on-amazon-emr/

Use Amazon Reviews as the standard dataset: https://s3.amazonaws.com/amazon-reviews-pds/readme.html