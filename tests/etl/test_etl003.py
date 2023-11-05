import sys

import pytest

from my_glue.common.config import Config, ConfigType
from my_glue.etl.etl003 import Etl
from my_glue.utils.s3_utils import upload_dir_or_file, download_s3_bucket


def test_run(glue_context, caplog, tmpdir, local_pre, upload_data, s3):
    sys.argv.append("--action_date=20231111")
    config = Config(ConfigType.S3, "ryo-input0", "etl003.ini", None)
    etl = Etl(glue_context, config)
    etl.run()
    download_s3_bucket(s3, "ryo-output1", f"{local_pre}/download/ryo-output1")


@pytest.fixture(scope="function")
def upload_data(s3, local_pre):
    # shutil.rmtree("download")
    upload_dir_or_file(f"{local_pre}/tests-resources/etl003/config", s3, "ryo-input0")
    upload_dir_or_file(f"{local_pre}/tests-resources/etl003/input1", s3, "ryo-input1")
    upload_dir_or_file(f"{local_pre}/tests-resources/etl003/input2", s3, "ryo-input2")
