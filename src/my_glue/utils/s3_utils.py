import os
from boto3 import client as s3_client


def check_s3_file_exist(s3: s3_client, bucket: str, path: str) -> bool:
    """
    Check if a file exists in an S3 bucket at a given path.

    Args:
        s3 (boto3.client): the S3 client.
        bucket (str): the S3 bucket.
        path (str): the S3 path.

    Returns:
        bool: True if the file exists, False otherwise.
    """

    try:
        s3.head_object(Bucket=bucket, Key=path)
        return True
    except Exception:
        return False


def check_s3_file_or_dir_exist(s3: s3_client, bucket: str, path: str) -> bool:
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
    return "Contents" in response


def delete_s3_file(s3: s3_client, bucket: str, path: str) -> None:
    """
    Delete a file from an S3 bucket at a given path.

    Args:
        s3 (boto3.client): the S3 client.
        bucket (str): the S3 bucket.
    Returns:
        None
    """
    s3.delete_object(Bucket=bucket, Key=path)


def rename_s3_file(s3: s3_client, bucket: str, old_path: str, new_path: str) -> None:
    """
    Rename a file from an S3 bucket at a given path.

    Args:
        s3 (boto3.client): the S3 client.
        bucket (str): the S3 bucket.
    Returns:
        None
    """
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": old_path},
        Key=new_path,
    )
    s3.delete_object(Bucket=bucket, Key=old_path)


def upload_file(s3: s3_client, file_name: str, bucket: str, object_name: str) -> None:
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3.upload_file(file_name, bucket, object_name)
