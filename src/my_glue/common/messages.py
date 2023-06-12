import my_glue.utils.conifg_utils as conifg_utils


class Message:
    def __init__(self, msg_path="src/config/message.ini"):
        self.config = conifg_utils.load_config(msg_path)

    def get_msg(self, section, key):
        return conifg_utils.read_config_value_by_section_key(self.config, section, key)
