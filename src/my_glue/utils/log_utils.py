import datetime
import logging
from logging.handlers import RotatingFileHandler

import pytz


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    file_handler = RotatingFileHandler("/home/glue_user/workspace/my_glue/log.log", encoding="utf-8", maxBytes=2 * 1024 * 1024, backupCount=1)
    file_handler.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    formatter.converter = lambda *args: datetime.datetime.now(pytz.timezone("Asia/Tokyo")).timetuple()

    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
