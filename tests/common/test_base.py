from my_glue.common.base import Base
from my_glue.common.output_source import OutputSource
from awsglue.context import GlueContext, DataFrame
from boto3 import client as s3_client
from my_glue.common.job_config import JobConfig
import sys


class TestBase(Base):
    def __init__(self, context: GlueContext, s3: s3_client, config: JobConfig) -> None:
        super().__init__(context, s3, config)

    def handle_data(self) -> DataFrame:
        return None


class OutputFile(OutputSource):
    def output_data(self, df: DataFrame) -> None:
        return None


def test_new(glue_context, s3):
    sys.argv.append("--JOB_NAME='test'")
    sys.argv.append("--JOB_NAME")
    sys.argv.append("test")
    config = JobConfig(output_source=OutputFile())
    TestBase(glue_context, s3, config).run()
