from my_glue.utils.log_utils import get_logger

logger = get_logger(__name__)


def exception_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            logger.info(f"Start {func.__name__}")
            res = func(*args, **kwargs)
            logger.info(f"End {func.__name__}")
            return res
        except Exception as e:
            logger.error(e)
            raise e


class BizException(Exception):
    """
    business exception
    """

    def __init__(self, message):
        super().__init__(message)


class ParamNotFoundException(Exception):
    """
    parameter not found exception
    """

    def __init__(self, message):
        super().__init__("parameter [{0}] is not found".format(message))


class FileNotFoundException(Exception):
    """
    file not found exception
    """

    def __init__(self, message):
        super().__init__(message)
