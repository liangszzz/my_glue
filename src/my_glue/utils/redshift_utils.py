from awsglue.context import DataFrame
from typing import Dict
from my_glue.utils import glue_utils


def get_data_from_redshift(
    url: str, user: str, password: str, table: str, redshiftTmpDir: Dict[None, str]
) -> DataFrame:
    my_conn_options = {
        "simpleSql": True,
        "url": url,
        "dbtable": table,
        "user": user,
        "password": password,
        "redshiftTmpDir": redshiftTmpDir,
    }
    glueContext = glue_utils.get_glue_context()
    df = glueContext.create_data_frame_from_options("redshift", my_conn_options)
    return df
