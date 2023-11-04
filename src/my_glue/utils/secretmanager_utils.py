from typing import Union

from boto3 import client

from my_glue.common.exceptions import BizException


def get_value(secret_name: str, region_name: str = "ap-northeast-1", enpoint_url: Union[str, None] = None) -> str:
    if enpoint_url is None:
        secretsmanager = client("secretsmanager", region_name=region_name)
    else:
        secretsmanager = client("secretsmanager", region_name=region_name, endpoint_url=enpoint_url)

    response = secretsmanager.get_secret_value(SecretId=secret_name)
    if "SecretString" in response:
        return response["SecretString"]

    raise BizException("Secret not found")
