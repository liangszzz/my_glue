import sys

from awsglue.context import DataFrame
from boto3 import client

from my_glue.common.base import Base
from my_glue.common.input_source import InputSource
from my_glue.common.job_config import JobConfig
from my_glue.common.output_source import OutputSource
from my_glue.utils import log_utils
from my_glue.utils import s3_utils as s3_utils
from tests.conftest import glue_context

logger = log_utils.get_logger(__name__)


def test_new(glue_context):
    spark1 = glue_context.spark_session
    spark2 = glue_context.spark_session
    assert spark1 == spark2, "one glue context create two spark session is equal"


def test_boto(s3):
    s3_utils.uploadDirOrFile("tests/resources/input/common/", s3, "test-resource")
    assert s3_utils.check_s3_file_or_dir_exist(s3, "test-resource", "common"), "common dir exists"
    assert s3_utils.check_s3_file_or_dir_exist(s3, "test-resource", "etl001") is False, "etl001 dir exists"


class TestBase(Base):
    def __init_(self, context: glue_context, s3: client, config: JobConfig):
        super.__init__(context, s3, config)

    def handle_data(self) -> DataFrame:
        return self.context.createDataFrame([], "id string")


class TestInputSource(InputSource):
    def __init__(self) -> None:
        super().__init__()

    def load_data(self) -> None:
        pass


class TestOutputSource(OutputSource):
    def __init__(self) -> None:
        super().__init__()

    def output_data(self, df: DataFrame):
        pass


def test_base_run_success(glue_context, s3):
    etl = TestBase(
        glue_context,
        s3,
        JobConfig(required_params=["JOB_NAME"], input_source=[TestInputSource()], output_source=[TestOutputSource()]),
    )
    etl.run()
    assert True


def test_base_run_fail(glue_context, s3):
    logger.error(sys.argv)
    etl = TestBase(glue_context, s3, JobConfig(required_params=["JOB_NAME", "CURRENT_USER"]))
    try:
        etl.run()
    except Exception as e:
        logger.error(e)
        assert e is not None
