import configparser

from awsglue.context import GlueContext
from boto3 import client as s3_client

from my_glue.common.input_source import InputFile
from my_glue.common.job_config import JobConfig
from my_glue.common.output_source import OutputFile
from my_glue.utils import log_utils


class InputOutputWithConfig:
    def __init__(self, context: GlueContext, s3: s3_client, config_path: str) -> None:
        super().__init__()
        self.context = context
        self.spark = context.spark_session
        self.s3 = s3
        self.logger = log_utils.get_logger(type(self).__name__)
        _config = configparser.ConfigParser()
        _config.read(config_path)
        self.config = _config
        self.job_config = JobConfig()
        self.init_config()

    def init_config(self):
        self.job_config.required_params = self.config.get("common", "required_params").split(",")
        self.job_config.job_start_msg = self.config.get("common", "job_start_msg")
        self.job_config.job_commit_msg = self.config.get("common", "job_commit_msg")
        self.load_input_config(self.config.get("common", "input"))
        self.load_output_config(self.config.get("common", "output"))

    def load_input_config(self, input: str) -> None:
        input_arr = input.split(",")
        for item in input_arr:
            input_type = self.config.get(item, "type")
            if input_type == "s3-file":
                self.job_config.input_source.append(
                    InputFile(
                        context=self.context,
                        s3=self.s3,
                        bucket=self.config.get(item, "bucket"),
                        path=self.config.get(item, "path"),
                        table_name=self.config.get(item, "table_name"),
                        required=self.config.getboolean(item, "required"),
                        file_not_exist_msg=self.config.get(item, "file_not_exist_msg"),
                        file_count_is_0_msg=self.config.get(item, "file_count_is_0_msg"),
                        file_count_msg=self.config.get(item, "file_count_msg"),
                        default_schema=self.config.get(item, "default_schema"),
                    )
                )

    def load_output_config(self, output: str) -> None:
        output_arr = output.split(",")
        for item in output_arr:
            if item == "s3-file":
                self.job_config.output_source.append(
                    OutputFile(
                        context=self.context,
                        s3=self.s3,
                        bucket=self.config.get(item, "bucket"),
                        path=self.config.get(item, "path"),
                        file_count_msg=self.config.get(item, "file_count_msg"),
                        file_export_success_msg=self.config.get(item, "file_export_success_msg"),
                        file_export_failed_msg=self.config.get(item, "file_export_failed_msg"),
                    )
                )

    def get_job_config(self) -> JobConfig:
        return self.job_config
