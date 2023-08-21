from my_glue.etl.etl002 import Etl
from my_glue.utils.s3_utils import upload_dir_or_file, download_s3_bucket


def test_run(glue_context, s3, caplog):
    upload_dir_or_file("tests/resources/etl002/input1", s3, "ryo-input1")
    upload_dir_or_file("tests/resources/etl002/input2", s3, "ryo-input2")

    etl = Etl(glue_context, s3, "tests/resources/etl002/config/etl002.ini")
    try:
        etl.run()
    except Exception as e:
        print(e)
    download_s3_bucket(s3, "ryo-input1", "download/input1")
    download_s3_bucket(s3, "ryo-input2", "download/input2")
    download_s3_bucket(s3, "ryo-output1", "download/output1")
