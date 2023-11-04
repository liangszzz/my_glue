from my_glue.common.exceptions import (BizException, FileNotFoundException,
                                       ParamNotFoundException,
                                       exception_decorator)


@exception_decorator
def error_fun(index: int) -> bool:
    if index == 0:
        raise BizException("error 1")
    elif index == 1:
        raise ParamNotFoundException("error 2")
    elif index == 2:
        raise FileNotFoundException("error 3")
    elif index == 99999:
        raise Exception("error other")

    return True


def test_biz_exception():
    try:
        error_fun(0)
    except BizException as e:
        assert "Biz exception: error 1" in str(e)


def test_param_not_found_exception():
    try:
        error_fun(1)
    except ParamNotFoundException as e:
        assert "parameter [error 2] is not found" in str(e)


def test_file_not_found_exception():
    try:
        error_fun(2)
    except FileNotFoundException as e:
        assert "file [error 3] is not found" in str(e)


def test_other_exception():
    try:
        error_fun(99999)
    except Exception as e:
        assert "error other" in str(e)


def test_no_exception():
    assert error_fun(1000000)
