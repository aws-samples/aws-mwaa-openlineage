# AWS CDK - Datalake
 
Use the AWS CDK to create a data lake.

-----
### Set up the environment

At a bash terminal session.

```bash
# clone the repo
git clone ssh://git.amazon.com/pkg/AwsCdkDatalake
# move to directory
cd AwsCdkDatalake
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

Use the AWS CDK to deploy an Amazon VPC across multiple availability zones.

```bash
# deploy the vpc stack
(.env)$ cdk deploy elkk-vpc
```

-----
## Amazon EMR

Add this for tuning?
https://aws.amazon.com/blogs/big-data/tune-hadoop-and-spark-performance-with-dr-elephant-and-sparklens-on-amazon-emr/

Use Amazon Reviews as the standard dataset: https://s3.amazonaws.com/amazon-reviews-pds/readme.html