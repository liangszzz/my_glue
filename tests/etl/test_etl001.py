import pytest
import sys

from my_glue.common.config import Config, ConfigType
from my_glue.etl.etl001 import Etl
from my_glue.utils.s3_utils import upload_dir_or_file, download_s3_bucket


def test_run_csv(glue_context, s3, caplog, tmpdir, local_pre, upload_data):
    sys.argv.append("--action_date=20231020")
    sys.argv.append("--input_file_type=s3-dir-csv")
    sys.argv.append("--input_file_bucket=ryo-input1")
    sys.argv.append("--input_file_path=path/001")
    sys.argv.append("--output_file_bucket=ryo-output1")
    sys.argv.append("--output_file_path=path/001")
    sys.argv.append("--decimal_columns=age")
    sys.argv.append("--date_columns=birthday")
    sys.argv.append("--date_fromat=yyyy-MM-dd")

    config = Config(ConfigType.S3, "ryo-input0", "etl001.ini", None)
    etl = Etl(glue_context, config)
    etl.run()
    download_s3_bucket(s3, "ryo-output1", f"{local_pre}/download/ryo-output1")


def test_run_tsv(glue_context, s3, caplog, tmpdir, local_pre, upload_data):
    sys.argv.append("--action_date=20231020")
    sys.argv.append("--input_file_type=s3-dir-tsv")
    sys.argv.append("--input_file_bucket=ryo-input1")
    sys.argv.append("--input_file_path=path/002")
    sys.argv.append("--output_file_bucket=ryo-output1")
    sys.argv.append("--output_file_path=path/002")
    sys.argv.append("--decimal_columns=age")
    sys.argv.append("--date_columns=birthday")
    sys.argv.append("--date_fromat=yyyy-MM-dd")

    config = Config(ConfigType.S3, "ryo-input0", "etl001.ini", None)
    etl = Etl(glue_context, config)
    etl.run()
    download_s3_bucket(s3, "ryo-output1", f"{local_pre}/download/ryo-output1")


def test_run_txt(glue_context, s3, caplog, tmpdir, local_pre, upload_data):
    sys.argv.append("--action_date=20231020")
    sys.argv.append("--input_file_type=s3-dir-txt")
    sys.argv.append("--input_file_bucket=ryo-input1")
    sys.argv.append("--input_file_path=path/003")
    sys.argv.append("--output_file_bucket=ryo-output1")
    sys.argv.append("--output_file_path=path/003")
    sys.argv.append("--decimal_columns=age")
    sys.argv.append("--date_columns=birthday")
    sys.argv.append("--date_fromat=yyyy-MM-dd")

    config = Config(ConfigType.S3, "ryo-input0", "etl001.ini", None)
    etl = Etl(glue_context, config)
    etl.run()
    download_s3_bucket(s3, "ryo-output1", f"{local_pre}/download/ryo-output1")


@pytest.fixture(scope="function")
def upload_data(s3, local_pre):
    # shutil.rmtree("download")
    upload_dir_or_file(f"{local_pre}/tests-resources/etl001/config", s3, "ryo-input0")
    upload_dir_or_file(f"{local_pre}/tests-resources/etl001/input1", s3, "ryo-input1")
