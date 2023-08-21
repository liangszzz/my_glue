from abc import ABC, abstractmethod

from awsglue.context import DataFrame, GlueContext
from boto3 import client

import my_glue.common.exceptions as exceptions
import my_glue.utils.glue_utils as glue_utils
import my_glue.utils.s3_utils as s3_utils
from my_glue.utils import log_utils


class InputSource(ABC):
    @abstractmethod
    def load_data(self) -> DataFrame:
        """
        get data from source
        """
        pass


class InputS3FileSource(InputSource):
    def __init__(
        self,
        context: GlueContext,
        s3: client,
        bucket: str,
        path: str,
        table_name: str,
        required: bool = True,
        file_not_exist_msg: str = "File does not exist",
        file_count_is_0_msg: str = "File {0} count is 0",
        file_count_msg: str = "File count is {0}",
        default_schema: str = None,
    ) -> None:
        """
        input source csv file

        Args:
            context (GlueContext): glue context
            s3 (client): s3 client
            bucket (str): s3 bucket
            path (str): s3 path
            table_name (str): table name in spark sql
            required (bool): required
            file_not_exist_msg (str): file not exist message
            file_count_is_0_msg (str): file count is 0 message
            file_count_msg (str): file count message
            default_schema (str): default schema
        """
        self.context = context
        self.s3 = s3
        self.bucket = bucket
        self.path = path
        self.table_name = table_name
        self.required = required
        self.file_not_exist_msg = file_not_exist_msg
        self.file_count_is_0_msg = file_count_is_0_msg
        self.file_count_msg = file_count_msg
        self.logger = log_utils.get_logger(type(self).__name__)
        self.default_schema = default_schema

    def load_data(self) -> DataFrame:
        file_exists = s3_utils.check_s3_file_or_dir_exist(self.s3, self.bucket, self.path)
        if file_exists is False and self.required is True:
            raise exceptions.FileNotFoundException(self.file_not_exist_msg.format(f"s3://{self.bucket}/{self.path}"))
        elif file_exists is False and self.required is False:
            df = self.context.createDataFrame([], self.default_schema)
            df.createOrReplaceTempView(self.table_name)
            self.logger.info(self.file_not_exist_msg)
            return df

        df = glue_utils.load_df_from_s3_csv(self.context, f"s3://{self.bucket}/{self.path}")
        if df.count() == 0 and self.required is True:
            self.logger.info(self.file_count_is_0_msg.format(self.table_name))
            return None
        self.logger.info(self.file_count_msg.format(df.count()))
        df.createOrReplaceTempView(self.table_name)
        return df


class InputCatlogSource(InputSource):
    def __init__(
        self,
        context: GlueContext,
        database: str,
        table_name: str,
        required: bool = True,
        table_not_exist_msg: str = "Table does not exist {0}",
        table_count_is_0_msg: str = "Table {0} count is 0",
        table_count_msg: str = "Table count is {0}",
        default_schema: str = None,
    ) -> None:
        self.context = context
        self.database = database
        self.table_name = table_name
        self.required = required
        self.table_not_exist_msg = table_not_exist_msg
        self.table_count_is_0_msg = table_count_is_0_msg
        self.table_count_msg = table_count_msg

    def load_data(self) -> DataFrame:
        df = self.context.create_data_frame_from_catalog(database=self.database, table_name=self.table_name)
        if df.count() == 0 and self.required is True:
            self.logger.info(self.table_count_is_0_msg.format(self.table_name))
            return None
        self.logger.info(self.table_count_msg.format(df.count()))
        return df
