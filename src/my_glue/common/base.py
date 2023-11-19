import sys
from abc import ABC
from typing import Any, Dict, List, Union

from awsglue.context import DataFrame, GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from my_glue.common import exceptions
from my_glue.common.config import Config, ConfigFile, InputOutputConfig, InputOutType
from my_glue.utils import glue_utils, s3_utils, sys_utils, time_utils
from my_glue.utils.log_utils import get_logger
from my_glue.utils.s3_utils import get_client


class Base(ABC):
    def __init__(self, context: GlueContext, config: Config) -> None:
        self.args = {}
        self.job = None
        self.context = context
        self.spark = context.spark_session
        self.logger = get_logger(type(self).__name__)
        self.optional_params: List[str] = []
        self.require_params: List[str] = []
        self.config_dict: Dict[str, str] = {}
        self.config = config

        self.defalut_csv_options = {
            "header": "true",
            "encoding": "utf-8",
            "quote": '"',
            "escape": '"',
            "quoteAll": "true",
        }

        self.defalut_tsv_options = {
            "header": "true",
            "encoding": "utf-8",
            "quote": '"',
            "escape": '"',
            "quoteAll": "true",
            "delimiter": "\t",
        }

        self.defalut_parquet_options = {}

        self.defalut_fixed_options = {
            "header": "false",
            "encoding": "utf-8",
        }

    def init_config(self) -> None:
        self.config_dict = self.config.load_config()

    def logger_start(self) -> None:
        if ConfigFile.LOGGER_START_MSG.value in self.config_dict:
            self.logger.info(self.config_dict[ConfigFile.LOGGER_START_MSG.value])
        return None

    def init_optional_params(self) -> None:
        if ConfigFile.OPTIONAL_PARAMS.value in self.config_dict:
            arr = self.config_dict[ConfigFile.OPTIONAL_PARAMS.value].split(",")
            for item in arr:
                if sys_utils.check_sys_arg_exists(item, "--"):
                    self.optional_params.append(item)
        return None

    def init_required_params(self) -> None:
        if ConfigFile.REQUIRED_PARAMS.value in self.config_dict:
            self.require_params.extend(self.config_dict[ConfigFile.REQUIRED_PARAMS.value].split(","))
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
        if "action_date" in self.args:
            date_obj = time_utils.str_to_date(self.args["action_date"], time_utils.FORMAT_YYYYMMDD)
        else:
            date_obj = time_utils.get_today()
        self.action_date = time_utils.get_date_str(date_obj, time_utils.FORMAT_YYYYMMDD)

    def commit_job(self) -> None:
        self.job.commit()  # type: ignore

    def load_data(self) -> None:
        return None

    def handle_data(self) -> None:
        return None

    def export_data(self) -> None:
        return None

    def logger_end(self) -> None:
        if ConfigFile.LOGGER_END_MSG.value in self.config_dict:
            self.logger.info(self.config_dict[ConfigFile.LOGGER_END_MSG.value])
        return None

    @exceptions.exception_decorator
    def run(self) -> None:
        self.init_config()
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

    def load_s3_file(
        self,
        section: str,
        cache_flag: bool = False,
        create_view_flag: bool = True,
        format_map: Union[None, Dict[str, str]] = None,
        optional_args: Union[None, Dict[str, Any]] = None,
    ) -> DataFrame:
        bucket = self.config_dict[f"{section}.{InputOutputConfig.BUCKET.value}"]
        prefix = self.config_dict[f"{section}.{InputOutputConfig.PATH.value}"]
        view_name = self.config_dict[f"{section}.{InputOutputConfig.TABLE_NAME.value}"]
        required = self.config_dict[f"{section}.{InputOutputConfig.REQUIRED.value}"]
        schema = self.config_dict[f"{section}.{InputOutputConfig.SCHEMA.value}"]
        file_type = self.config_dict[f"{section}.{InputOutputConfig.TYPE.value}"]
        if format_map is not None:
            bucket = bucket.format_map(format_map)
            prefix = prefix.format_map(format_map)
            view_name = view_name.format_map(format_map)
            required = required.format_map(format_map)
            schema = schema.format_map(format_map)
            file_type = file_type.format_map(format_map)

        s3 = get_client()

        file_format = "csv"
        if "parquet" in file_type:
            file_format = "parquet"

        if s3_utils.check_s3_file_or_dir_exist(s3, bucket, prefix):
            df = glue_utils.load_df_from_s3(
                self.context, f"s3://{bucket}/{prefix}", file_format=file_format, options=optional_args
            )
        else:
            if required == "True":
                raise exceptions.S3FileNotExistException(f"s3://{bucket}/{prefix}")
            else:
                df = self.context.createDataFrame([], schema)

        if cache_flag:
            df.cache()
        if create_view_flag:
            df.createOrReplaceTempView(view_name)
        return df

    def export_to_s3(
        self,
        section: str,
        df: DataFrame,
        format_map: Union[None, Dict[str, str]] = None,
        optional_args: Union[None, Dict[str, Any]] = None,
    ) -> None:
        if optional_args is not None:
            optional_args["compression"] = "gzip"

        file_type = self.config_dict[f"{section}.{InputOutputConfig.TYPE.value}"]
        bucket = self.config_dict[f"{section}.{InputOutputConfig.BUCKET.value}"]
        prefix = self.config_dict[f"{section}.{InputOutputConfig.PATH.value}"]
        if format_map is not None:
            bucket = bucket.format_map((format_map))
            prefix = prefix.format_map((format_map))
            file_type = file_type.format_map((format_map))

        if InputOutType.S3_DIR_PARQUET.value == file_type:
            glue_utils.export_data_frame_to_parquet(df, f"s3://{bucket}/{prefix}", options=optional_args)
        elif "csv" in file_type:
            glue_utils.export_data_frame_to_csv_dir(df, f"s3://{bucket}/{prefix}", options=optional_args)
        else:
            raise Exception("not use exception")
        return None
