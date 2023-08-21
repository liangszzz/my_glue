from my_glue.etl.etl002 import Etl
from my_glue.utils.s3_utils import upload_dir_or_file, download_s3_bucket


def test_run(glue_context, s3):
    upload_dir_or_file("tests/resources/etl002/input", s3, "ryo-input1")
    Etl(glue_context, s3, "tests/resources/etl002/config/etl002.ini").run()
    download_s3_bucket(s3, "ryo-output1", "download")
