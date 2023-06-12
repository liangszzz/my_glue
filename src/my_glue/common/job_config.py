import sys
from typing import List, Dict, Any


from awsglue.utils import getResolvedOptions

import my_glue.common.exceptions as exceptions
from my_glue.common.input_source import InputSource
from my_glue.common.output_source import OutputSource

import my_glue.utils.sys_utils as sys_utils


class JobConfig:
    def __init__(
        self,
        required_params: List[str] = ["JOB_NAME"],
        job_start_msg: str = "Job start",
        job_commit_msg: str = "Job commit",
        input_source: List[InputSource] = [],
        output_source: OutputSource = None,
    ) -> None:
        self._required_params = required_params
        self.input_source: List[InputSource] = input_source
        self.output_source: OutputSource = output_source
        self.job_start_msg = job_start_msg
        self.job_commit_msg = job_commit_msg
        self.check_required_params()
        self.resolve_params()

    def get_job_name(self) -> str:
        return self.args["JOB_NAME"]

    def get_args(self) -> dict:
        return self.args

    def check_required_params(self) -> None:
        # check required params
        for param in self._required_params:
            if not sys_utils.check_sys_arg_exists(param, "--"):
                raise exceptions.ParamNotFoundException(param)

    def resolve_params(self) -> Dict[str, Any]:
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    def add_input_source(self, input_source: InputSource) -> None:
        self.input_source.append(input_source)

    def set_output_source(self, output_source: OutputSource) -> None:
        self.output_source = output_source
