from awsglue.context import GlueContext

from my_glue.utils import glue_utils, log_utils

logger = log_utils.get_logger(__name__)


def test_get_data_frame_from_catalog(glue_context) -> None:
    try:
        df = glue_utils.get_data_frame_from_catalog(glue_context, "test", "test")
        assert df is not None
    except Exception as e:
        logger.error(e)
        assert e is not None


def test_create_null_df(glue_context: GlueContext) -> None:
    df = glue_context.createDataFrame([], "id string")
    df.show()
    df2 = glue_context.sparkSession.createDataFrame([], "id string")
    df2.show()
    assert True
