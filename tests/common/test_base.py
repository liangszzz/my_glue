def test_new(glue_context):
    spark1 = glue_context.spark_session
    spark2 = glue_context.spark_session
    assert spark1 == spark2, "one glue context create two spark session is equal"
