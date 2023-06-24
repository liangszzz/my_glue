import sys
from abc import ABC, abstractmethod
from typing import List

from awsglue.context import DataFrame, GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from boto3 import client

from my_glue.common import exceptions
from my_glue.common.job_config import JobConfig
from my_glue.utils import sys_utils
from my_glue.utils.log_utils import get_logger


class Base(ABC):
    def __init__(self, context: GlueContext, s3: client, config: JobConfig) -> None:
        self.context = context
        self.spark = context.spark_session
        self.s3 = s3
        self.job_config = config
        self.logger = get_logger(type(self).__name__)
        self.params: List[str] = []

    def init_optional_params(self, params: List[str] = []) -> None:
        self.params = params

    def check_required_params(self) -> None:
        for param in self.job_config.required_params:
            if not sys_utils.check_sys_arg_exists(param, "--"):
                raise exceptions.ParamNotFoundException(param)

    def init_job(self) -> None:
        self.logger.info(self.job_config.job_start_msg)
        self.job = Job(self.context)
        self.params.extend(self.job_config.required_params)
        self.args = getResolvedOptions(sys.argv, self.params)
        self.job.init(self.args["JOB_NAME"], self.args)

    def commit_job(self) -> None:
        self.job.commit()
        self.logger.info(self.job_config.job_commit_msg)

    def load_data(self) -> None:
        for input in self.job_config.input_source:
            input.load_data()

    @abstractmethod
    def handle_data(self) -> DataFrame:
        pass

    def export_source(self, df: DataFrame) -> None:
        for output in self.job_config.output_source:
            output.output_data(df)

    def run(self) -> None:
        self.init_optional_params()
        self.check_required_params()
        self.init_job()
        self.load_data()
        self.handle_data()
        self.export_source(self.handle_data())
        self.commit_job()
