### Infrastructure provisioning using AWS CDK and AWS Cloudshell

The template has the following prerequisites: 

* An AWS account (https://console.aws.amazon.com/console/home)
* Amazon Linux 2 with AWS CDK (https://aws.amazon.com/getting-started/guides/setup-cdk/module-two/) and Docker CLI (https://docs.aws.amazon.com/lambda/latest/dg/images-create.html) installed. Alternatively, setting up an AWS Cloud9 environment (https://docs.aws.amazon.com/cloud9/latest/user-guide/create-environment-main.html) will satisfy this requirement.

*Deployment steps*

Clone Github repository and install python dependencies. Bootstrap CDK (https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html) if required. 

```bash
git clone https://github.com/aws-samples/aws-mwaa-openlineage (Not currently available)
git clone https://gitlab.aws.dev/pvillena/aws-mwaa-openlineage (Internal)
cd aws-mwaa-openlineage
python3 -m venv .env
source ./scripts/install-deps.sh
cdk bootstrap
```

* Update the value for the variable EXTERNAL_IP in constants.py to your outbound IP for connecting to the internet. This will configure security groups so that you can access Marquez but block other clients. constants.py is found in the root folder of the cloned repository.

* Set variable to outbound IP for connecting to the internet.
EXTERNAL_IP = "255.255.255.255"

Deploy VPC_S3 Stack. This stack provisions a new VPC dedicated for this solution as well as the security groups that will be used by the different components. It will create a new S3 bucket and upload the source raw data based on the tickit sample database. This serves as the landing area from the OLTP database. We then need to parse the metadata of these files through a Glue crawler and this will facilitate the native integration between Amazon Redshift and the Amazon S3 data lake.
```bash
cdk deploy vpc-s3
```
Deploy Lineage Stack. This stack creates an EC2 instance that will host the Marquez (OpenLineage) (https://marquezproject.ai/) web application. Access the Marquez web UI through the url, https://{ec2.public_dns_name}:3000/. This url is also available as part of the CDK outputs for the lineage stack.
```bash
cdk deploy marquez
```
Deploy Redshift Stack. This stack creates a Redshift Serverless (https://aws.amazon.com/redshift/redshift-serverless/) endpoint.
```bash
cdk deploy redshift
```
Deploy MWAA Stack. This stack creates an MWAA environment and is the last step of the deployment. You can access the MWAA UI (https://docs.aws.amazon.com/mwaa/latest/userguide/access-airflow-ui.html) through the url provided in CDK output. 
```bash
cdk deploy mwaa
```
Test-run of sample data pipeline

On MWAA, there is already an example data pipeline deploys which consists of two DAGs. It builds a star schema on top of the TICKIT sample database (https://docs.aws.amazon.com/redshift/latest/dg/c_sampledb.html). One DAG is responsible for loading data from the S3 data lake into a Redshift staging layer while the second DAG loads data from the staging layer to the dimensional model.
[Image: Image.jpg]Open MWAA UI through the URL obtained the in deployment step 6) and launch the following DAG <Name>. As part of the run, lineage metadata will be send to the Marquez.

After the DAG has been executed, open Marquez’s URL obtained in deployment step 4). On Marquez, you will find lineage metadata for the computed star schema and according data assets on Redshift.

*Clean up*

Delete the CDK stacks to avoid ongoing charges for the resources that you have created. Run the cdk destroy command  in the aws-mwaa-openlineage project directory so that all resources are undeployed.
```bash
cdk destroy --all
```
