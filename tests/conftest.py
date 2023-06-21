import os
import pytest
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
import sys
from boto3 import client as s3_client


@pytest.fixture(scope="session", autouse=True)
def glue_context():
    spark_context = (
        SparkSession.builder.config(
            "spark.hadoop.fs.s3a.endpoint", "http://localstack:4566"
        )
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.region", "ap-northeast-1")
        .config("spark.hadoop.fs.s3a.format", "json")
        .getOrCreate()
    )

    return GlueContext(spark_context)


@pytest.fixture(scope="session", autouse=True)
def s3():
    return s3_client(
        "s3",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="ap-northeast-1",
        use_ssl=False,
    )


@pytest.fixture(scope="function", autouse=True)
def clear_sys_argv():
    sys.argv.clear()
    sys.argv.append("--JOB_NAME")
    sys.argv.append("test")
    sys.argv.append("--JOB_NAME=test")



@pytest.fixture(scope="session", autouse=True)
def s3_init(s3):
    bucket = "test-resource"
    local_path = "tests/resources"
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

    for root, _, files in os.walk(local_path):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = os.path.join("", os.path.relpath(local_path, local_path))
            s3.upload_file(local_path, bucket, s3_key)
    response = s3.list_objects_v2(Bucket=bucket)
    if "Contents" in response:
        objects = response["Contents"]
        for obj in objects:
            file_key = obj["Key"]
            file_size = obj["Size"]
            print(f"File: {file_key}, Size: {file_size} bytes")
