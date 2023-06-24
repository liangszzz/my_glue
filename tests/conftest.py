import sys

import pytest
from awsglue.context import GlueContext
from boto3 import client as client
from pyspark.sql import SparkSession

from my_glue.utils import log_utils

bucket = "test-resource"
database_name = "test"

logger = log_utils.get_logger(__name__)


@pytest.fixture(scope="session")
def glue_context():
    logger.info("--------------------------start glue_context init---------------------------")
    spark_context = (
        SparkSession.builder.config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.region", "ap-northeast-1")
        .config("spark.hadoop.fs.s3a.format", "json")
        .getOrCreate()
    )

    return GlueContext(spark_context)


@pytest.fixture(scope="session")
def s3():
    logger.info("--------------------------start s3 init---------------------------")
    return client(
        "s3",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="ap-northeast-1",
        use_ssl=False,
    )


@pytest.fixture(scope="function", autouse=True)
def clear_sys_argv():
    logger.info("--------------------------start args init---------------------------")
    sys.argv.clear()
    sys.argv.append("--JOB_NAME")
    sys.argv.append("test")
    sys.argv.append("--JOB_NAME=test")
    logger.info("--------------------------end args init---------------------------")


@pytest.fixture(scope="session", autouse=True)
def s3_bucket_init(s3):
    logger.info("--------------------------start s3 bucket init---------------------------")
    try:
        s3.head_bucket(Bucket=bucket)
        response = s3.list_objects_v2(Bucket=bucket)
        if "Contents" in response:
            objects = response["Contents"]
            for obj in objects:
                file_key = obj["Key"]
                s3.delete_object(Bucket=bucket, Key=file_key)
        s3.delete_bucket(Bucket=bucket)
    except Exception:
        pass

    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={"LocationConstraint": "ap-northeast-1"},
    )
    logger.info("--------------------------end s3 bucket init---------------------------")


def catalog_init(glue_context):
    logger.info("--------------------------start glue database init---------------------------")
    glue_client = client(
        "glue",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="ap-northeast-1",
        use_ssl=False,
    )

    response = glue_client.create_database(DatabaseInput={"Name": database_name})
    logger.info(response)
    logger.info("--------------------------end glue database init---------------------------")
