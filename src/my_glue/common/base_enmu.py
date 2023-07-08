from enum import Enum


class InputOutputType(Enum):
    S3_DIR = "s3-dir"
    S3_CSV = "s3-csv"
    S3_TEXT = "s3-text"
    LOCAL_FILE = "local-file"
    CATALOG = "catalog"
