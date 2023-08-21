from awsglue.context import GlueContext
from boto3 import client

from my_glue.common.input_source import InputS3FileSource
from my_glue.common.job_config import JobConfig
from my_glue.common.output_source import OutputFile
from my_glue.utils import log_utils, sys_utils


class InputOutputWithConfig:
    def __init__(self, context: GlueContext, s3: client, config_path: str) -> None:
        super().__init__()
        self.context = context
        self.spark = context.spark_session
        self.s3 = s3
        self.logger = log_utils.get_logger(type(self).__name__)
        self.job_config = JobConfig()
        self.config = sys_utils.read_config_to_json(config_path)
        self.init_config()

    def init_config(
        self,
    ) -> None:
        common = self.config.get("common")
        self.job_config.required_params = common["required_params"].split(",")
        self.job_config.job_start_msg = common["job_start_msg"]
        self.job_config.job_end_msg = common["job_end_msg"]
        self.load_input_config(common["input"])
        self.load_output_config(common["output"])

    def load_input_config(self, input: str) -> None:
        input_arr = input.split(",")
        for item in input_arr:
            input_type = self.config[item]["type"]
            if input_type == "s3-file":
                self.job_config.input_source.append(
                    InputS3FileSource(
                        context=self.context,
                        s3=self.s3,
                        bucket=self.config[item]["bucket"],
                        path=self.config[item]["path"],
                        table_name=self.config[item]["table_name"],
                        required=bool(self.config[item]["required"]),
                        file_not_exist_msg=self.config[item]["file_not_exist_msg"],
                        file_count_is_0_msg=self.config[item]["file_count_is_0_msg"],
                        file_count_msg=self.config[item]["file_count_msg"],
                        default_schema=self.config[item]["default_schema"],
                    )
                )

    def load_output_config(self, output: str) -> None:
        output_arr = output.split(",")
        for item in output_arr:
            input_type = self.config[item]["type"]
            if input_type == "s3-dir":
                self.job_config.output_source.append(
                    OutputFile(
                        context=self.context,
                        s3=self.s3,
                        bucket=self.config[item]["bucket"],
                        path=self.config[item]["path"],
                        file_count_msg=self.config[item]["file_count_msg"],
                        file_export_success_msg=self.config[item]["file_export_success_msg"],
                        file_export_failed_msg=self.config[item]["file_export_failed_msg"],
                    )
                )

    def get_job_config(self) -> JobConfig:
        return self.job_config
