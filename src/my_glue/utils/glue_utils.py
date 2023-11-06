import uuid
from typing import Any, Dict, List, Union

from awsglue import DynamicFrame
from awsglue.context import DataFrame, GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType

from my_glue.utils.s3_utils import get_client, rename_s3_file


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


def load_df_from_s3(
        context: GlueContext,
        s3_path: Union[str, List[str]],
        options: Dict[str, Any] = {"header": "true", "encoding": "utf-8", "quote": '"', "escape": '"'},
        format: str = "csv",
        schema: Union[StructType, str] = None,
) -> DataFrame:
    """
    Creates a DataFrame from a CSV file stored on S3 using GlueContext. Takes in three parameters:
    Args:
        context (GlueContext): The Glue context object.
        s3_path (str): The S3 Full path. s3://bucket/path or [s3://bucket/path, s3://bucket/path]
        format (str): The format of the file, defaults to csv
        schema (str): The schema
        options (Dict[str, Any]): The options
    Returns:
        DataFrame: The resulting `DataFrame`.
    """
    if schema is None:
        return context.spark_session.read.format(format).options(**options).load(s3_path)
    else:
        return context.spark_session.read.format(format).options(**options).load(s3_path, schema=schema)


def load_df_from_s3_csv_with_schema(
        context: GlueContext,
        s3_path: str,
        schema: StructType,
        options: Dict[str, Any] = {"header": "true", "encoding": "utf-8", "quote": '"', "escape": '"'},
) -> DataFrame:
    """
    Creates a `DataFrame` from a CSV file stored on S3.
    Args:
        context (GlueContext): The Glue context object.
        s3_path (str): The S3 path.
        schema (StructType): The schema.
        options (Dict[str, Any]): The options
    Returns:
        DataFrame: The resulting `DataFrame`.
    """
    return context.spark_session.read.csv(s3_path, schema, **options)


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


def export_data_frame_to_csv_dir(
        df: DataFrame,
        s3_path: str,
        repartition: Union[int, None] = None,
        max_records_per_file: int = 500000,
        options: Dict[str, Any] = {
            "encoding": "utf-8",
            "quote": '"',
            "quoteAll": True,
            "header": "true",
            "escape": '"',
            "ignoreLeadingWhiteSpace": True,
            "ignoreTrailingWhiteSpace": True,
        },
) -> None:
    """
    Exports a specified pandas DataFrame, `df`, to a CSV file stored on S3

    Args:
        df (DataFrame): The DataFrame.
        s3_path (str): The S3 path.
        repartition (int): The number of partitions.
        options (Dict[str, Any]): The options
        max_records_per_file (int): The max records per file

    Returns:
        None
    """
    if repartition is not None:
        df = df.repartition(repartition)
    df.write.mode("overwrite").option("maxRecordsPerFile", max_records_per_file).csv(s3_path, **options)


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
    s3_tmp_path = f"s3://{bucket}/{uuid_str}"
    s3 = get_client()
    df.write.mode("overwrite").csv(s3_tmp_path, **options)
    response = s3.list_objects(Bucket=bucket, Prefix=f"{uuid_str}")
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].startswith(f"{uuid_str}/part"):
                rename_s3_file(s3, bucket, bucket, obj["Key"], s3_path, True)


def check_df_count_is_zero(df: DataFrame) -> bool:
    return df.select("1").limit(1).count() == 0
