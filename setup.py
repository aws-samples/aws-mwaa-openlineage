import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="AwsCdkDatalake",
    version="0.0.1",
    description="Build an Amazon S3 Data Lake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="author",
    package_dir={"": "stacks"},
    packages=setuptools.find_packages(where="stacks"),
    install_requires=[
        "awscli",
        "aws-cdk.core",
        "aws_cdk.aws_cloudformation",
        "aws_cdk.aws_dynamodb",
        "aws_cdk.aws_emr",
        "aws_cdk.aws_glue",
        "aws_cdk.aws_iam",
        "aws_cdk.aws_lakeformation",
        "aws_cdk.aws_lambda",
        "aws_cdk.aws_logs",
        "aws_cdk.aws_s3",
        "aws_cdk.aws_s3_deployment",
        "boto3",
        "pytz",
    ],
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
