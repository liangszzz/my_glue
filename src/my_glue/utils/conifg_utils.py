from configparser import ConfigParser


def get_config(config_file: str) -> ConfigParser:
    """
    Read and parse a configuration file.

    Args:
        config_file (str): the path to the configuration file.

    Returns:
        ConfigParser: the parsed configuration file.
    """

    # Create a ConfigParser object.
    config = ConfigParser()

    # Read the configuration file.
    config = config.read(config_file)

    # Return the parsed configuration file.
    return config


def read_config_value_by_section_key(
    config: ConfigParser, section: str, key: str
) -> str:
    """
    Read the value of a configuration file by section and key.

    Args:
        config (ConfigParser): the configuration file.
        section (str): the section of the configuration file.
        key (str): the key of the configuration file.

    Returns:
        str: the value of the configuration file.
    """
    return config.get(section, key)
