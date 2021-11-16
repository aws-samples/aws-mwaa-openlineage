# get modules
import os
import logging
import boto3
import requests
import json
from openlineage.client import OpenLineageClient

# set glue client
client = boto3.client("glue")


def handler(event, context):

    logging.getLogger().setLevel(logging.INFO)

    logging.info(f"Input event: {event}")

    # get env vars
    OPENLINEAGE_API = os.getenv("OPENLINEAGE_API")
    OPENLINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE")

    # url for the namespaces call
    url = f"{OPENLINEAGE_API}/api/v1/namespaces"
    time_out = 5

    # send the request
    try:
        resp = requests.get(url, timeout=time_out)
        logging.info(f"resp.status_code: {resp.status_code}")
        logging.info(f"resp.reason: {resp.reason}")
        logging.info(f"resp.content: {resp.content}")

    except requests.Timeout:
        logging.warning(f"Request to {url} timed out after {time_out} seconds")

    """ table update event
    {
        "version": "0",
        "id": "6beeac3f-e09c-b403-0912-c3e55902f601",
        "detail-type": "Glue Data Catalog Table State Change",
        "source": "aws.glue",
        "account": "820406222077",
        "time": "2021-11-15T07:38:23Z",
        "region": "us-east-1",
        "resources": [
            "arn:aws:glue:us-east-1:820406222077:table/cdkdl-dev/cdkdl_dev_foundationstoragef3_s3bucketraw88473b86_cxz3i1e716d3"
        ],
        "detail": {
            "databaseName": "cdkdl-dev",
            "changedPartitions": ["[nyctaxi]"],
            "typeOfChange": "BatchCreatePartition",
            "tableName": "cdkdl_dev_foundationstoragef3_s3bucketraw88473b86_cxz3i1e716d3",
        },
    }
    """

    # get the glue table definition
    response = client.get_table(
        DatabaseName=event["detail"]["databaseName"], Name=event["detail"]["tableName"]
    )
    logging.info(response)

    """ glue get_table response
    {
        "Table": {
            "Name": "cdkdl_dev_foundationstoragef3_s3bucketraw88473b86_cxz3i1e716d3",
            "DatabaseName": "cdkdl-dev",
            "Owner": "owner",
            "CreateTime": datetime.datetime(2021, 11, 15, 20, 38, 22, tzinfo=tzlocal()),
            "UpdateTime": datetime.datetime(2021, 11, 15, 20, 38, 22, tzinfo=tzlocal()),
            "LastAccessTime": datetime.datetime(
                2021, 11, 15, 20, 38, 22, tzinfo=tzlocal()
            ),
            "Retention": 0,
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "vendorid", "Type": "bigint"},
                    {"Name": "lpep_pickup_datetime", "Type": "string"},
                    {"Name": "lpep_dropoff_datetime", "Type": "string"},
                    {"Name": "store_and_fwd_flag", "Type": "string"},
                    {"Name": "ratecodeid", "Type": "bigint"},
                    {"Name": "pulocationid", "Type": "bigint"},
                    {"Name": "dolocationid", "Type": "bigint"},
                    {"Name": "passenger_count", "Type": "bigint"},
                    {"Name": "trip_distance", "Type": "double"},
                    {"Name": "fare_amount", "Type": "double"},
                    {"Name": "extra", "Type": "double"},
                    {"Name": "mta_tax", "Type": "double"},
                    {"Name": "tip_amount", "Type": "double"},
                    {"Name": "tolls_amount", "Type": "double"},
                    {"Name": "ehail_fee", "Type": "string"},
                    {"Name": "improvement_surcharge", "Type": "double"},
                    {"Name": "total_amount", "Type": "double"},
                    {"Name": "payment_type", "Type": "bigint"},
                    {"Name": "trip_type", "Type": "bigint"},
                    {"Name": "congestion_surcharge", "Type": "double"},
                ],
                "Location": "s3://cdkdl-dev-foundationstoragef3-s3bucketraw88473b86-cxz3i1e716d3/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "Parameters": {"field.delim": ","},
                },
                "BucketColumns": [],
                "SortColumns": [],
                "Parameters": {
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "CrawlerSchemaSerializerVersion": "1.0",
                    "UPDATED_BY_CRAWLER": "crawlerraw-k5DIcIH9II8T",
                    "areColumnsQuoted": "false",
                    "averageRecordSize": "153",
                    "classification": "csv",
                    "columnsOrdered": "true",
                    "compressionType": "none",
                    "delimiter": ",",
                    "objectCount": "1",
                    "recordCount": "37183",
                    "sizeKey": "5689051",
                    "skip.header.line.count": "1",
                    "typeOfData": "file",
                },
                "StoredAsSubDirectories": False,
            },
            "PartitionKeys": [{"Name": "partition_0", "Type": "string"}],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "CrawlerSchemaDeserializerVersion": "1.0",
                "CrawlerSchemaSerializerVersion": "1.0",
                "UPDATED_BY_CRAWLER": "crawlerraw-k5DIcIH9II8T",
                "areColumnsQuoted": "false",
                "averageRecordSize": "153",
                "classification": "csv",
                "columnsOrdered": "true",
                "compressionType": "none",
                "delimiter": ",",
                "objectCount": "1",
                "recordCount": "37183",
                "sizeKey": "5689051",
                "skip.header.line.count": "1",
                "typeOfData": "file",
            },
            "CreatedBy": "arn:aws:sts::820406222077:assumed-role/cdkdl-dev-batchglueBB9040D3-crawlerrole40EA2665-YAX8ADPUTE9S/AWS-Crawler",
            "IsRegisteredWithLakeFormation": False,
            "CatalogId": "820406222077",
        },
        "ResponseMetadata": {
            "RequestId": "ba11b823-3303-4be2-a4b0-3e774fefc9fb",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "date": "Tue, 16 Nov 2021 04:11:19 GMT",
                "content-type": "application/x-amz-json-1.1",
                "content-length": "2612",
                "connection": "keep-alive",
                "x-amzn-requestid": "ba11b823-3303-4be2-a4b0-3e774fefc9fb",
            },
            "RetryAttempts": 0,
        },
    }
    """

    # get the glue table version definition
    response = client.get_table_version(
        DatabaseName=event["detail"]["databaseName"],
        TableName=event["detail"]["tableName"],
        VersionId="0",
    )
    logging.info(response)

    """ get_table_version response
    {
        "Table": {
            "Name": "cdkdl_dev_foundationstoragef3_s3bucketraw88473b86_cxz3i1e716d3",
            "DatabaseName": "cdkdl-dev",
            "Owner": "owner",
            "CreateTime": datetime.datetime(2021, 11, 15, 20, 38, 22, tzinfo=tzlocal()),
            "UpdateTime": datetime.datetime(2021, 11, 15, 20, 38, 22, tzinfo=tzlocal()),
            "LastAccessTime": datetime.datetime(
                2021, 11, 15, 20, 38, 22, tzinfo=tzlocal()
            ),
            "Retention": 0,
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "vendorid", "Type": "bigint"},
                    {"Name": "lpep_pickup_datetime", "Type": "string"},
                    {"Name": "lpep_dropoff_datetime", "Type": "string"},
                    {"Name": "store_and_fwd_flag", "Type": "string"},
                    {"Name": "ratecodeid", "Type": "bigint"},
                    {"Name": "pulocationid", "Type": "bigint"},
                    {"Name": "dolocationid", "Type": "bigint"},
                    {"Name": "passenger_count", "Type": "bigint"},
                    {"Name": "trip_distance", "Type": "double"},
                    {"Name": "fare_amount", "Type": "double"},
                    {"Name": "extra", "Type": "double"},
                    {"Name": "mta_tax", "Type": "double"},
                    {"Name": "tip_amount", "Type": "double"},
                    {"Name": "tolls_amount", "Type": "double"},
                    {"Name": "ehail_fee", "Type": "string"},
                    {"Name": "improvement_surcharge", "Type": "double"},
                    {"Name": "total_amount", "Type": "double"},
                    {"Name": "payment_type", "Type": "bigint"},
                    {"Name": "trip_type", "Type": "bigint"},
                    {"Name": "congestion_surcharge", "Type": "double"},
                ],
                "Location": "s3://cdkdl-dev-foundationstoragef3-s3bucketraw88473b86-cxz3i1e716d3/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "Parameters": {"field.delim": ","},
                },
                "BucketColumns": [],
                "SortColumns": [],
                "Parameters": {
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "CrawlerSchemaSerializerVersion": "1.0",
                    "UPDATED_BY_CRAWLER": "crawlerraw-k5DIcIH9II8T",
                    "areColumnsQuoted": "false",
                    "averageRecordSize": "153",
                    "classification": "csv",
                    "columnsOrdered": "true",
                    "compressionType": "none",
                    "delimiter": ",",
                    "objectCount": "1",
                    "recordCount": "37183",
                    "sizeKey": "5689051",
                    "skip.header.line.count": "1",
                    "typeOfData": "file",
                },
                "StoredAsSubDirectories": False,
            },
            "PartitionKeys": [{"Name": "partition_0", "Type": "string"}],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "CrawlerSchemaDeserializerVersion": "1.0",
                "CrawlerSchemaSerializerVersion": "1.0",
                "UPDATED_BY_CRAWLER": "crawlerraw-k5DIcIH9II8T",
                "areColumnsQuoted": "false",
                "averageRecordSize": "153",
                "classification": "csv",
                "columnsOrdered": "true",
                "compressionType": "none",
                "delimiter": ",",
                "objectCount": "1",
                "recordCount": "37183",
                "sizeKey": "5689051",
                "skip.header.line.count": "1",
                "typeOfData": "file",
            },
            "CreatedBy": "arn:aws:sts::820406222077:assumed-role/cdkdl-dev-batchglueBB9040D3-crawlerrole40EA2665-YAX8ADPUTE9S/AWS-Crawler",
            "IsRegisteredWithLakeFormation": False,
            "CatalogId": "820406222077",
        },
        "ResponseMetadata": {
            "RequestId": "d0be84ab-5035-4328-ba82-15bfb4646b41",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "date": "Tue, 16 Nov 2021 04:18:08 GMT",
                "content-type": "application/x-amz-json-1.1",
                "content-length": "2612",
                "connection": "keep-alive",
                "x-amzn-requestid": "d0be84ab-5035-4328-ba82-15bfb4646b41",
            },
            "RetryAttempts": 0,
        },
    }
    """

    # event build
    lineage_event = {
        "eventType": "COMPLETE",  #  "START", "COMPLETE", "ABORT", "FAIL", "OTHER"
        "eventTime": event["time"],  # "2020-12-09T23:37:31.081Z",
        "run": {
            "runId": event["id"],  # event id from the event bus message
        },
        "job": {  # can this to linked to the crawler job id?
            "namespace": OPENLINEAGE_NAMESPACE,
            "name": "myjob.mytask",
        },
        "inputs": [
            {
                "namespace": response["TableVersion"]["Table"]["StorageDescriptor"][
                    "Location"
                ],
                "name": response["TableVersion"]["Table"]["StorageDescriptor"][
                    "Location"
                ],
            }
        ],
        "outputs": [
            {
                "namespace": OPENLINEAGE_NAMESPACE,
                "name": f"instance.{response['TableVersion']['Table']['DatabaseName']}.{response['TableVersion']['Table']['Name']}",
                "facets": {
                    "documentation": None,
                    "dataSource": {
                        "_producer": "lyndon-thinkpad/127.0.1.1",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "s3://sql-runner",
                        "uri": "s3://sql-runner",
                    },
                    "schema": None,
                },
                "outputFacets": None,
            }
        ],
        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "schemaURL": "https://openlineage.io/spec/1-0-0/OpenLineage.json#/definitions/RunEvent",
    }
    logging.info(lineage_event)

    # construct the dataset put

    return lineage_event
