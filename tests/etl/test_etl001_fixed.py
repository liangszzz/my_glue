import sys

import pytest

from my_glue.common.config import Config, ConfigType
from my_glue.etl.etl001_fixed import Etl
from my_glue.utils.s3_utils import download_s3_bucket, upload_dir_or_file


def test_run_fixed(glue_context, s3, caplog, tmpdir, local_pre, upload_data):
    sys.argv.append("--action_date=20231020")
    sys.argv.append("--input_file_type=s3-fixed")
    sys.argv.append("--input_file_bucket=cdp-input1")
    sys.argv.append("--input_file_path=path/004")
    sys.argv.append("--output_file_bucket=cdp-output1")
    sys.argv.append("--output_file_path=path/004")
    sys.argv.append("--decimal_columns=age")
    sys.argv.append("--date_columns=birthday")
    sys.argv.append("--date_fromat=yyyy-MM-dd")
    sys.argv.append("--split_count=4,8,5,10,1")
    sys.argv.append("--split_name=id,name,age,birthday,address_code")
    sys.argv.append("--charset=shift-jis")

    config = Config(ConfigType.S3, "cdp-input0", "etl001_fixed.ini", None)
    etl = Etl(glue_context, config)
    etl.run()

    download_s3_bucket(s3, "cdp-output1", tmpdir)


@pytest.fixture(scope="function")
def upload_data(s3, local_pre):
    # shutil.rmtree("download")
    upload_dir_or_file(f"{local_pre}/tests-resources/etl001/config", s3, "cdp-input0")
    upload_dir_or_file(f"{local_pre}/tests-resources/etl001/input1", s3, "cdp-input1")
