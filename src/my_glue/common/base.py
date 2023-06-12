from abc import ABC, abstractmethod

from my_glue.common.job_config import JobConfig


from awsglue.context import GlueContext, DataFrame
from awsglue.job import Job
from boto3 import client as s3_client


from my_glue.utils.log_utils import get_logger


class Base(ABC):
    def __init__(self, context: GlueContext, s3: s3_client, config: JobConfig) -> None:
        self.context = context
        self.s3 = s3
        self.config = config
        self.logger = get_logger(type(self).__name__)

    def init_job(self) -> None:
        self.logger.info(self.config.job_start_msg)
        self.job = Job(self.context)
        self.job.init(self.config.get_job_name(), self.config.get_args())

    def commit_job(self) -> None:
        self.job.commit()
        self.logger.info("Commit job")

    def load_data(self) -> None:
        for input in self.config.input_source:
            input.get_data()

    @abstractmethod
    def handle_data(self) -> DataFrame:
        pass

    def export_source(self, df: DataFrame) -> None:
        self.config.output_source.output_data(df)

    def run(self) -> None:
        self.init_job()
        self.config.check_required_params()
        self.config.resolve_params()
        self.load_data()
        self.handle_data()
        self.export_source(self.handle_data())
        self.commit_job()
