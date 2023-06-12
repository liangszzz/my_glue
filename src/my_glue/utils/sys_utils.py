import sys


def check_sys_arg_exists(arg: str, prefix: str) -> bool:
    """
    Check if the given argument exists.

    Args:
        arg (str): the argument to check.
        prefix (str): the prefix of the argument.
    Returns:
        bool: True if the argument exists, False otherwise.
    """
    for param in sys.argv:
        if param.startswith(f"{prefix}{arg}"):
            return True
    return False
