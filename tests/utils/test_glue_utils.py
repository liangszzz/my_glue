from my_glue.utils import glue_utils, log_utils, s3_utils

logger = log_utils.get_logger(__name__)


def test_get_data_frame_from_catalog(glue_context) -> None:
    try:
        df = glue_utils.get_data_frame_from_catalog(glue_context, "test", "test")
        assert df is not None
    except Exception as e:
        logger.error(e)
        assert e is not None


def test_export_data_frame_to_csv(glue_context, s3) -> None:
    data = [
        {"id": 1, "name": "Alice", "age": 28},
        {"id": 2, "name": "Bob", "age": 35},
        {"id": 3, "name": "Charlie", "age": 22},
    ]
    df = glue_context.createDataFrame(data)
    glue_utils.export_data_frame_to_csv(df, s3, "ryo-output1", "output.csv")
    s3_utils.download_s3_bucket(s3, "ryo-output1", "download")
