from typing import Any
from my_glue.common.input_source import InputSource
from my_glue.common.job_config import JobConfig
from my_glue.common.output_source import OutputSource
from my_glue.utils.log_utils import log_utils
from my_glue.utils.conifg_utils import conifg_utils
from awsglue.context import GlueContext, DataFrame


class InputWithConfig(InputSource, OutputSource):
    def __init__(self, context: GlueContext, config_path: str) -> None:
        super().__init__()
        self.logger = log_utils.get_logger(type(self).__name__)
        self.config = conifg_utils.get_config(config_path)

    def init_config(self):
        pass

    def load_data(self) -> DataFrame:
        pass

    def output_data(self, df: DataFrame) -> None:
        pass

    def get_job_config(self) -> JobConfig:
        return None
