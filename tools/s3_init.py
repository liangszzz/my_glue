import boto3

from my_glue.utils import s3_utils

s3 = boto3.client(
        "s3",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="ap-northeast-1",
        use_ssl=False,
    )

bucket = "ryozen-glue"


try:
    s3.head_bucket(Bucket=bucket)
except Exception:
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={"LocationConstraint": "ap-northeast-1"},
    )

s3_utils.uploadDirOrFile("tools/resources", s3, bucket)

response = s3.list_objects_v2(Bucket=bucket)
if "Contents" in response:
    objects = response["Contents"]
    for obj in objects:
        file_key = obj["Key"]
        print(file_key)
