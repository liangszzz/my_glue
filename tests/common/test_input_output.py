from my_glue.common.input_output_source import InputOutputWithConfig


def test_new_output_source(glue_context, s3):
    source = InputOutputWithConfig(glue_context, s3, "tests/resources/config/common/etl001.ini")
    config = source.get_job_config()
    assert source is not None
    assert config is not None
