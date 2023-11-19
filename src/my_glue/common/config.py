import configparser
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Union

from my_glue.utils.s3_utils import get_client, read_s3_file


class ConfigFile(Enum):
    LOGGER_START_MSG = "base.logger_start_msg"
    LOGGER_END_MSG = "base.logger_end_msg"
    REQUIRED_PARAMS = "base.required_params"
    OPTIONAL_PARAMS = "base.optional_params"


class InputOutType(Enum):
    # directory
    S3_DIR_CSV = "s3-dir-csv"
    S3_DIR_TSV = "s3-dir-tsv"
    S3_DIR_TXT = "s3-dir-txt"
    S3_DIR_PARQUET = "s3-dir-parquet"
    # table
    CATALOG = "catalog"
    MYSQL = "mysql"
    REDSHIFT = "redshift"
    # file
    S3_CSV = "s3-csv"
    S3_TSV = "s3-tsv"
    S3_TEXT = "s3-text"
    S3_FIXED = "s3-fixed"


class InputOutputConfig(Enum):
    TYPE = "type"
    BUCKET = "bucket"
    PATH = "path"
    TABLE_NAME = "table_name"
    REQUIRED = "required"
    SCHEMA = "schema"
    ERROR_MSG = "error_msg"
    SUCCESS_MSG = "success_msg"


class ConfigType(Enum):
    S3 = "s3"
    LOCAL = "local"


@dataclass
class Config:
    config_type: ConfigType
    bucket: Union[None, str]
    prefix: Union[None, str]
    file_path: Union[None, str]

    def load_config(self) -> Dict[str, str]:
        if self.config_type == ConfigType.S3 and self.bucket is not None and self.prefix is not None:
            return self.load_config_from_s3(self.bucket, self.prefix)
        elif self.config_type == ConfigType.LOCAL and self.file_path is not None:
            return self.load_config_from_file(self.file_path)
        else:
            raise Exception("config file read exception")

    def load_config_from_file(self, file_path: str) -> Dict[str, str]:
        config = configparser.ConfigParser()
        config.read(file_path)
        return self.read_config_to_dict(config)

    def load_config_from_s3(self, bucket: str, prefix: str) -> Dict[str, str]:
        content = read_s3_file(get_client(), bucket, prefix)
        config = configparser.ConfigParser()
        config.read_string(content)
        return self.read_config_to_dict(config)

    def read_config_to_dict(self, config: configparser.ConfigParser) -> Dict[str, str]:
        dict: Dict[str, str] = {}
        for section in config.sections():
            for option in config.options(section):
                dict[f"{section}.{option}"] = config.get(section, option)
        return dict
