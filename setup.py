import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="AwsCdkDatalake",
    version="0.0.1",
    description="Governed data lake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="mcgregf",
    package_dir={"AwsCdkDatalake": "."},
    packages=setuptools.find_packages(
        include=["batch", "foundation", "governance", "orchestration"]
    ),
    install_requires=[
        "aws_cdk.core",
        "aws_cdk.aws_athena",
        "aws_cdk.aws_cloudformation",
        "aws_cdk.aws_cloudtrail",
        "aws_cdk.aws_dynamodb",
        "aws_cdk.aws_emr",
        "aws_cdk.aws_events",
        "aws_cdk.aws_events_targets",
        "aws_cdk.aws_glue",
        "aws_cdk.aws_iam",
        "aws_cdk.aws_lakeformation",
        "aws_cdk.aws_lambda",
        "aws_cdk.aws_lambda_python",
        "aws_cdk.aws_logs",
        "aws_cdk.pipelines",
        "aws-cdk.aws_mwaa",
        "aws_cdk.aws_s3",
        "aws_cdk.aws_s3_deployment",
    ],
    extras_require={
        "dev": [
            "awscli",
            "boto3",
            "pip==21.3.1",
            # for vscode
            "black",
            # for lineage
            "openlineage-airflow",
            "google-cloud-bigquery",
            "great-expectations",
            "requests",
        ]
    },
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
