import sys
from abc import ABC
from typing import Dict, List

from awsglue.context import DataFrame, GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from boto3 import client

from my_glue.common import exceptions
from my_glue.common.config import (Config, ConfigFile, InputOutputConfig,
                                   InputOutType)
from my_glue.utils import glue_utils, s3_utils, sys_utils
from my_glue.utils.log_utils import get_logger


class Base(ABC):
    def __init__(self, context: GlueContext, s3: client) -> None:
        self.context = context
        self.spark = context.spark_session
        self.s3 = s3
        self.logger = get_logger(type(self).__name__)

        self.optional_params: List[str] = []
        self.require_params: List[str] = []

        self.dict: Dict[str, str] = {}

    def init_config(self, config: Config) -> None:
        self.dict = config.load_config()

    def logger_start(self) -> None:
        if ConfigFile.LOGGER_START_MSG.value in self.dict:
            self.logger.info(self.dict[ConfigFile.LOGGER_START_MSG.value])
        return None

    def init_optional_params(self) -> None:
        if ConfigFile.OPTIONAL_PARAMS.value in self.dict:
            arr = self.dict[ConfigFile.OPTIONAL_PARAMS.value].split(",")
            for item in arr:
                if sys_utils.check_sys_arg_exists(item, "--"):
                    self.optional_params.append(item)
        return None

    def init_required_params(self) -> None:
        if ConfigFile.REQUIRED_PARAMS.value in self.dict:
            self.require_params.extend(self.dict[ConfigFile.REQUIRED_PARAMS.value].split(","))
        return None

    def check_required_params(self) -> None:
        for param in self.require_params:
            if not sys_utils.check_sys_arg_exists(param, "--"):
                raise exceptions.ParamNotFoundException(param)

    def init_job(self) -> None:
        self.job = Job(self.context)
        self.optional_params.extend(self.require_params)

        self.args = getResolvedOptions(sys.argv, self.optional_params)
        self.job.init(self.args["JOB_NAME"], self.args)

    def handler_args(self) -> None:
        return None

    def commit_job(self) -> None:
        self.job.commit()

    def load_data(self) -> None:
        return None

    def handle_data(self) -> None:
        return None

    def export_data(self) -> None:
        return None

    def logger_end(self) -> None:
        if ConfigFile.LOGGER_END_MSG.value in self.dict:
            self.logger.info(self.dict[ConfigFile.LOGGER_END_MSG.value])
        return None

    @exceptions.exception_decorator
    def run(self) -> None:
        self.logger_start()
        self.init_optional_params()
        self.init_required_params()
        self.check_required_params()
        self.init_job()
        self.handler_args()
        self.load_data()
        self.handle_data()
        self.export_data()
        self.commit_job()
        self.logger_end()

    def load_s3_file(self, section: str, cache_flag: bool = False, create_view_flag: bool = True,
                     prefix_fuc=None) -> DataFrame:
        bucket = self.dict[f"{section}.{InputOutputConfig.BUCKET.value}"]

        prefix = self.dict[f"{section}.{InputOutputConfig.PATH.value}"]
        if prefix_fuc is not None:
            prefix = prefix_fuc(prefix)

        view_name = self.dict[f"{section}.{InputOutputConfig.TABLE_NAME.value}"]
        required = self.dict[f"{section}.{InputOutputConfig.REQUIRED.value}"]
        schema = self.dict[f"{section}.{InputOutputConfig.SCHEMA.value}"]

        file_type = self.dict[f"{section}.{InputOutputConfig.TYPE.value}"]

        if file_type == InputOutType.S3_DIR.value:
            if s3_utils.check_s3_file_or_dir_exist(self.s3, bucket, prefix):
                df = glue_utils.load_df_from_s3(self.context, f"s3://{bucket}/{prefix}")
            else:
                if required == "True":
                    raise exceptions.S3FileNotExistException(f"s3://{bucket}/{prefix}")
                else:
                    df = self.context.createDataFrame([], schema)

        elif file_type == InputOutType.S3_CSV.value:
            if s3_utils.check_s3_file_or_dir_exist(self.s3, bucket, prefix, dir=False):
                df = glue_utils.load_df_from_s3(self.context, f"s3://{bucket}/{prefix}")
            else:
                if required == "True":
                    raise exceptions.S3FileNotExistException(f"s3://{bucket}/{prefix}")
                else:
                    df = self.context.createDataFrame([], schema)

        elif file_type == InputOutType.S3_TEXT.value:
            if s3_utils.check_s3_file_or_dir_exist(self.s3, bucket, prefix, dir=False):
                df = glue_utils.load_df_from_s3_text(self.context, f"s3://{bucket}/{prefix}")
            else:
                if required == "True":
                    raise exceptions.S3FileNotExistException(f"s3://{bucket}/{prefix}")
                else:
                    df = self.context.createDataFrame([], schema)
        else:
            raise Exception("not support exception")
        if cache_flag:
            df.cache()
        if create_view_flag:
            df.createOrReplaceTempView(view_name)
        return df

    def export_to_s3(self, section: str, df: DataFrame) -> None:
        file_type = self.dict[f"{section}.{InputOutputConfig.TYPE.value}"]
        bucket = self.dict[f"{section}.{InputOutputConfig.BUCKET.value}"]
        prefix = self.dict[f"{section}.{InputOutputConfig.PATH.value}"]
        if file_type == InputOutType.S3_DIR.value:
            glue_utils.export_data_frame_to_csv_dir(df, f"s3://{bucket}/{prefix}")

        elif file_type == InputOutType.S3_CSV.value:
            glue_utils.export_data_frame_to_csv(df, self.s3, bucket, prefix)
        else:
            raise Exception("not use exception")
        return None
