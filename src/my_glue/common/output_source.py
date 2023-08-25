from abc import ABC, abstractmethod

from awsglue.context import DataFrame, GlueContext
from boto3 import client

import my_glue.utils.glue_utils as glue_utils
from my_glue.utils.log_utils import get_logger


class OutputSource(ABC):
    @abstractmethod
    def output_data(self, df: DataFrame) -> None:
        pass


class OutputFile(OutputSource):
    def __init__(
        self,
        context: GlueContext,
        s3: client,
        bucket: str,
        path: str,
        file_count_msg: str = "File count is {0}",
        file_export_success_msg: str = "File export success",
        file_export_failed_msg: str = "File export failed",
    ) -> None:
        self.context = context
        self.s3 = s3
        self.bucket = bucket
        self.path = path
        self.file_count_msg = file_count_msg
        self.file_export_success_msg = file_export_success_msg
        self.file_export_failed_msg = file_export_failed_msg
        self.logger = get_logger(type(self).__name__)

    def output_data(self, df: DataFrame) -> None:
        self.logger.info(self.file_count_msg.format(df.count()))
        try:
            glue_utils.export_data_frame_to_csv_dir(df, f"s3://{self.bucket}/{self.path}")
        except Exception as e:
            self.logger.error(self.file_export_failed_msg)
            raise e
        self.logger.info(self.file_export_success_msg)
