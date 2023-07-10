from typing import List

from my_glue.common.input_source import InputSource
from my_glue.common.output_source import OutputSource


class JobConfig:
    def __init__(
        self,
        required_params: List[str] = [],
        input_source: List[InputSource] = [],
        output_source: List[OutputSource] = [],
        job_start_msg: str = "job started",
        job_end_msg: str = "job finished",
    ) -> None:
        self._required_params: List[str] = required_params
        self._input_source: List[InputSource] = input_source
        self._output_source: List[OutputSource] = output_source
        self._job_start_msg: str = job_start_msg
        self._job_end_msg: str = job_end_msg

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
    def job_end_msg(self) -> str:
        return self._job_end_msg

    @job_end_msg.setter
    def job_end_msg(self, job_end_msg: str):
        self._job_end_msg = job_end_msg

    @property
    def input_source(self) -> List[InputSource]:
        return self._input_source

    @input_source.setter
    def input_source(self, input_source: InputSource) -> None:
        self._input_source.append(input_source)

    @property
    def output_source(self) -> List[OutputSource]:
        return self._output_source

    @output_source.setter
    def output_source(self, output_source: List[OutputSource]) -> None:
        self._output_source = output_source
