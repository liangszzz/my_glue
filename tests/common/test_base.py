
def test_new(glue_context):
    spark1 = glue_context.spark_session
    spark2 = glue_context.spark_session
    assert spark1 == spark2, "one glue context create two spark session is equal"

def test_boto(s3,s3_init):
    response=s3.list_objects_v2(Bucket="test-resource", Prefix="etl001")
    print(response['Contents'])
    assert False