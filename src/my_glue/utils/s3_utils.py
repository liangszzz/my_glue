import os

from boto3 import client


def get_client() -> client:
    return client("s3")


def check_s3_file_or_dir_exist(s3: client, bucket: str, path: str) -> bool:
    """
    Check if a file and directory exists in an S3 bucket at a given path.

    Args:
        s3 (boto3.client): the S3 client.
        bucket (str): the S3 bucket.
        path (str): the S3 path.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    response = s3.list_objects_v2(Bucket=bucket, Prefix=path)
    return any(obj["Key"] == path for obj in response.get("Contents", []))


def delete_s3_file(s3: client, bucket: str, path: str) -> None:
    """
    Delete a file from an S3 bucket at a given path.

    Args:
        s3 (boto3.client): the S3 client.
        bucket (str): the S3 bucket.
    Returns:
        None
    """
    s3.delete_object(Bucket=bucket, Key=path)


def rename_s3_file(
        s3: client, input_bucket: str, output_bucket: str, input_path: str, output_path: str, delete: bool
) -> None:
    """
    Rename a file from an S3 bucket at a given path.

    Args:
        s3 (boto3.client): the S3 client.
        bucket (str): the S3 bucket.
    Returns:
        None
    """
    s3.copy_object(
        Bucket=output_bucket,
        CopySource={"Bucket": input_bucket, "Key": input_path},
        Key=output_path,
    )
    if delete:
        s3.delete_object(Bucket=input_bucket, Key=input_path)


def upload_dir_or_file(local_path: str, s3: client, bucket: str):
    """
    Uploads a directory or file from the local machine to an S3 bucket recursively.

    :param local_path: The path of the directory or file to upload.
    :type local_path: str
    :param s3: The S3 client object.
    :type s3: boto3.client
    :param bucket: The name of the destination S3 bucket.
    :type bucket: str
    :return: None
    """
    for root, _, files in os.walk(local_path):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.isfile(file_path):
                s3_key = os.path.join("", os.path.relpath(file_path, local_path))
                s3.upload_file(file_path, bucket, s3_key)
            else:
                upload_dir_or_file(file_path, s3, bucket)


def download_s3_bucket(s3: client, bucket: str, local_path: str) -> None:
    """
    Downloads an S3 bucket recursively.
    Args:
        s3 (boto3.client): the S3 client.
        bucket (str): the S3 bucket.
    Returns:
        None
    """
    response = s3.list_objects_v2(Bucket=bucket)
    if "Contents" in response:
        for obj in response["Contents"]:
            obj_key = obj["Key"]
            destination_path = os.path.join(local_path, obj_key)
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            s3.download_file(bucket, obj_key, destination_path)
