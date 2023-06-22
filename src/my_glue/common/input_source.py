from abc import ABC, abstractmethod
from awsglue.context import GlueContext, DataFrame
from boto3 import client as s3_client
import my_glue.utils.glue_utils as glue_utils
import my_glue.utils.s3_utils as s3_utils
import my_glue.common.exceptions as exceptions


class InputSource(ABC):
    @abstractmethod
    def load_data(self) -> DataFrame:
        """
        get data from source
        """
        pass


class InputFile(InputSource):
    def __init__(
        self,
        context: GlueContext,
        s3: s3_client,
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
            s3 (s3_client): s3 client
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
        self.logger = logging.getLogger(type(self).__name__)
        self.default_schema = default_schema

    def load_data(self) -> DataFrame:
        file_exists = s3_utils.check_s3_file_or_dir_exist(
            self.s3, self.bucket, self.path
        )
        if file_exists is False and self.required is True:
            raise exceptions.FileNotFoundException(self.file_not_exist_msg)
        elif file_exists is False and self.required is False:
            df = self.context.createDataFrame([], self.default_schema)
            df.createOrReplaceTempView(self.table_name)
            self.logger.info(self.file_not_exist_msg)
            return df

        df = glue_utils.get_date_frame_from_s3_csv(
            self.context, f"s3://{self.bucket}/{self.path}"
        )
        if df.count() == 0 and self.required is True:
            self.logger.info(self.file_count_is_0_msg.format(self.table_name))
            return None
        self.logger.info(self.file_count_msg.format(df.count()))
        df.createOrReplaceTempView(self.table_name)
        df.show()
        return df
