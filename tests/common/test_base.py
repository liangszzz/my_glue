from my_glue.utils import s3_utils as s3_utils


def test_new(glue_context):
    spark1 = glue_context.spark_session
    spark2 = glue_context.spark_session
    assert spark1 == spark2, "one glue context create two spark session is equal"


def test_boto(s3, s3_init):
    assert s3_utils.check_s3_file_or_dir_exist(
        s3, "test-resource", "common"
    ), "common dir exists"
    assert s3_utils.check_s3_file_or_dir_exist(
        s3, "test-resource", "etl001"
    ), "etl001 dir exists"
