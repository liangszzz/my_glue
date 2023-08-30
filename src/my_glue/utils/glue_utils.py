import uuid
from typing import Any, Dict, List, Union

from awsglue import DynamicFrame
from awsglue.context import DataFrame, GlueContext
from boto3 import client
from pyspark.context import SparkContext

from my_glue.utils.s3_utils import rename_s3_file


def get_glue_context() -> GlueContext:
    return GlueContext(SparkContext())


def get_data_frame_from_catalog(context: GlueContext, database: str, table: str) -> DataFrame:
    """
    Creates a `DataFrame` from a Glue catalog database and table.

    Args:
        context (GlueContext): The Glue context object.
        database (str): The name of the database.
        table (str): The name of the table.

    Returns:
        DataFrame: The resulting `DataFrame`.
    """
    return context.create_data_frame.from_catalog(database, table)


def load_df_from_s3_csv(
    context: GlueContext,
    s3_path: Union[str, List[str]],
    options: Dict[str, Any] = {"header": "true", "encoding": "utf-8", "quote": '"', "escape": '"'},
) -> DataFrame:
    """
    Creates a DataFrame from a CSV file stored on S3 using GlueContext. Takes in three parameters:
    Args:
        context (GlueContext): The Glue context object.
        s3_path (str): The S3 path.
        format_options (dict): The format options

    Returns:
        DataFrame: The resulting `DataFrame`.
    """
    return context.spark_session.read.format("csv").options(**options).load(s3_path)


def load_df_from_s3_text(context: GlueContext, s3_path: str) -> DataFrame:
    """
    Creates a `DataFrame` from a text file stored on S3.
    Args:
        context (GlueContext): The Glue context object.
        s3_path (str): The S3 path.
    Returns:
        DataFrame: The resulting `DataFrame`.
    """
    return context.spark_session.read.text(s3_path)


def convert_data_frame_to_dynamic_frame(context: GlueContext, df: DataFrame, name: str) -> DynamicFrame:
    """
    Converts a specified pandas DataFrame, `df`, to a Glue DynamicFrame

    Args:
        context (GlueContext): The Glue context object.
        df (DataFrame): The pandas DataFrame.
        name (str): The name of the Glue DynamicFrame.

    Returns:
        DynamicFrame: The resulting Glue DynamicFrame
    """
    return DynamicFrame.fromDF(df, context, name)


def convert_dynamic_frame_to_data_frame(context: GlueContext, df: DynamicFrame, name: str) -> DataFrame:
    """
    Converts a specified Glue DynamicFrame, `df`, to a pandas DataFrame

    Args:
        context (GlueContext): The Glue context object.
        df (DynamicFrame): The Glue DynamicFrame.
        name (str): The name of the pandas DataFrame.

    Returns:
        DataFrame: The resulting pandas DataFrame
    """
    return df.toDF(context, name)


def export_data_frame_to_csv_dir(
    df: DataFrame,
    s3_path: str,
    repartition: int = 10,
    options: Dict[str, Any] = {
        "encoding": "utf-8",
        "quote": '"',
        "quoteAll": True,
        "header": "true",
        "escape": '"',
    },
) -> None:
    """
    Exports a specified pandas DataFrame, `df`, to a CSV file stored on S3

    Args:
        df (DataFrame): The pandas DataFrame.
        s3_path (str): The S3 path.
        repartition (int): The number of partitions.
        options (Dict[str, Any]): The options

    Returns:
        None
    """
    df = df.repartition(repartition)
    df.write.mode("overwrite").csv(s3_path, **options)


def export_data_frame_to_catalog(
    df: DataFrame,
    database: str,
    table: str,
    options: Dict[str, Any] = {"encoding": "utf-8"},
) -> None:
    """
    Exports a specified pandas DataFrame, `df`, to a Glue catalog database and table
    """
    df.write.format("glueparquet").mode("overwrite").options(**options).saveAsTable(database, table)


def export_data_frame_to_csv(
    df: DataFrame,
    s3: client,
    bucket: str,
    s3_path: str,
    options: Dict[str, Any] = {
        "encoding": "utf-8",
        "quote": '"',
        "quoteAll": True,
        "header": "true",
        "escape": '"',
    },
) -> None:
    df = df.repartition(1)
    uuid_str = str(uuid.uuid4())
    s3_tmp_path = f"s3://{bucket}/tmp/{uuid_str}"
    df.write.mode("overwrite").csv(s3_tmp_path, **options)
    response = s3.list_objects(Bucket=bucket, Prefix=f"tmp/{uuid_str}")
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].startswith(f"tmp/{uuid_str}/part"):
                rename_s3_file(s3, bucket, bucket, obj["Key"], s3_path, True)
