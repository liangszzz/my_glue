import pytest

from my_glue.common.config import Config, ConfigType
from my_glue.etl.etl002 import Etl
from my_glue.utils import glue_utils
from my_glue.utils.glue_utils import load_df_from_s3
from my_glue.utils.s3_utils import download_s3_bucket, upload_dir_or_file


def test_run(glue_context, s3, caplog, tmpdir, local_pre, upload_data):
    config = Config(ConfigType.S3, s3, "ryo-input0", "etl002.ini", None)
    etl = Etl(glue_context, s3, config)
    try:
        etl.run()
    except Exception as e:
        print(e)

    dfs = etl.export_df.randomSplit([0.5, 0.5])
    for i in range(0, len(dfs)):
        df = dfs[i]
        glue_utils.export_data_frame_to_csv(df, s3, "ryo-output1", f"output/split/df_{i}.csv")

    download_s3_bucket(s3, "ryo-input0", f"{local_pre}/download/input0")
    download_s3_bucket(s3, "ryo-input1", f"{local_pre}/download/input1")
    download_s3_bucket(s3, "ryo-input2", f"{local_pre}/download/input2")
    download_s3_bucket(s3, "ryo-output1", f"{local_pre}/download/output1")


def test_run2(glue_context, s3, caplog, local_pre):
    upload_dir_or_file(f"{local_pre}/tests/resources/etl002/input1", s3, "ryo-input1")
    df1 = load_df_from_s3(glue_context, "s3://ryo-input1/etl002/user.csv")
    df2 = load_df_from_s3(glue_context, "s3://ryo-input1/etl002/user2.csv")

    df = df1.join(df2, "id", "full")
    df.show()


@pytest.fixture(scope="function")
def upload_data(s3, local_pre):
    # shutil.rmtree("download")
    upload_dir_or_file(f"{local_pre}tests/resources/etl002/config", s3, "ryo-input0")
    upload_dir_or_file(f"{local_pre}tests/resources/etl002/input1", s3, "ryo-input1")
    upload_dir_or_file(f"{local_pre}tests/resources/etl002/input2", s3, "ryo-input2")
