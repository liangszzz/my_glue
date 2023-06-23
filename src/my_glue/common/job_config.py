from typing import List

from my_glue.common.input_source import InputSource
from my_glue.common.output_source import OutputSource


class JobConfig:
    def __init__(self) -> None:
        self._required_params: List[str] = []
        self._input_source: List[InputSource] = []
        self._output_source: OutputSource = []
        self._job_start_msg: str = ""
        self._job_commit_msg: str = ""

    @property
    def required_params(self) -> List[str]:
        return self._required_params

    @required_params.setter
    def required_params(self, required_params: List[str]):
        self._required_params = required_params

    @property
    def job_start_msg(self) -> str:
        return self._job_start_msg

    @job_start_msg.setter
    def job_start_msg(self, job_start_msg: str):
        self._job_start_msg = job_start_msg

    @property
    def job_commit_msg(self) -> str:
        return self._job_commit_msg

    @job_commit_msg.setter
    def job_commit_msg(self, job_commit_msg: str):
        self._job_commit_msg = job_commit_msg

    @property
    def input_source(self) -> List[InputSource]:
        return self._input_source

    @input_source.setter
    def input_source(self, input_source: InputSource) -> None:
        self._input_source.append(input_source)

    @property
    def output_source(self) -> OutputSource:
        return self._output_source

    @output_source.setter
    def output_source(self, output_source: OutputSource) -> None:
        self._output_source = output_source
