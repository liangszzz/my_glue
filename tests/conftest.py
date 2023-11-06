import os
import shutil
import sys

import pytest
from awsglue.context import GlueContext, SparkSession

from my_glue.utils import log_utils
from my_glue.utils.s3_utils import get_client

input_bucket = "ryo-input"
output_bucket = "ryo-output"

endpoint_url = "http://localstack:4566"
aws_access_key_id = "test"
aws_secret_access_key = "test"
region_name = "ap-northeast-1"

logger = log_utils.get_logger(__name__)


current_module_path = os.getcwd()
index = current_module_path.index("my_glue")
current_module_path = current_module_path[0:index + 7]

@pytest.fixture(scope="function", autouse=True)
def clear_sys_argv():
    logger.info("--------------------------start args init---------------------------")
    sys.argv.clear()
    sys.argv.append("--JOB_NAME")
    sys.argv.append("test")
    sys.argv.append("--JOB_NAME=test")
    sys.argv.append("--dev")
    logger.info("--------------------------end args init---------------------------")


@pytest.fixture(scope="session")
def glue_context():
    spark_context = (
        SparkSession.builder.config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
        .config("fs.s3a.access.key", aws_access_key_id)
        .config("fs.s3a.secret.key", aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.region", region_name)
        .config("spark.hadoop.fs.s3a.format", "json")
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("fs.s3a.path.style.access", "true")
        .config("fs.s3a.connection.ssl.enabled", "false")
        .config("com.amazonaws.services.s3a.enableV4", "true")
        .getOrCreate()
    )
    spark_context.sparkContext.setLogLevel("INFO")

    return GlueContext(spark_context)


@pytest.fixture(scope="session")
def s3():
    logger.info("--------------------------start s3 init---------------------------")
    sys.argv.append("--dev")
    return get_client()


@pytest.fixture(scope="function", autouse=True)
def s3_handler(s3):
    s3_delete_bucket(s3)
    s3_create_bucket(s3)


@pytest.fixture(scope="session", autouse=False)
def local_pre():
    try:
        shutil.rmtree("/home/glue_user/workspace/my_glue/download")
    except:
        pass
    return current_module_path


def s3_create_bucket(s3):
    logger.info("--------------------------start s3 bucket create---------------------------")
    for i in range(0, 10):
        try:
            s3.head_bucket(Bucket=input_bucket + str(i))
        except Exception:
            s3.create_bucket(
                Bucket=input_bucket + str(i),
                CreateBucketConfiguration={"LocationConstraint": region_name},
            )
    for i in range(0, 10):
        try:
            s3.head_bucket(Bucket=output_bucket + str(i))
        except Exception:
            s3.create_bucket(
                Bucket=output_bucket + str(i),
                CreateBucketConfiguration={"LocationConstraint": region_name},
            )
    logger.info("--------------------------end s3 bucket create---------------------------")


def s3_delete_bucket(s3):
    logger.info("--------------------------start s3 bucket delete---------------------------")
    for i in range(0, 10):
        try:
            response = s3.list_objects_v2(Bucket=input_bucket + str(i))
            if "Contents" in response:
                objects = response["Contents"]
                for obj in objects:
                    file_key = obj["Key"]
                    s3.delete_object(Bucket=input_bucket + str(i), Key=file_key)
            s3.delete_bucket(Bucket=input_bucket + str(i))
        except:
            pass
    for i in range(0, 10):
        try:
            response = s3.list_objects_v2(Bucket=output_bucket + str(i))
            if "Contents" in response:
                objects = response["Contents"]
                for obj in objects:
                    file_key = obj["Key"]
                    s3.delete_object(Bucket=output_bucket + str(i), Key=file_key)
            s3.delete_bucket(Bucket=output_bucket + str(i))
        except:
            pass
    logger.info("--------------------------end s3 bucket delete---------------------------")
