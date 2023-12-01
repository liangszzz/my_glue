from awsglue.context import GlueContext

from my_glue.common.base import Base
from my_glue.common.config import Config, ConfigType
from my_glue.utils import glue_utils


class Etl(Base):
    def __init__(self, context: GlueContext, config: Config) -> None:
        super().__init__(context, config)

    def load_data(self) -> None:
        self.input_df = self.load_s3_file("input1", False, False, None)
        self.input_df.show()

    def export_data(self) -> None:
        self.export_to_s3("output1", self.input_df)


if __name__ == "__main__":
    context = glue_utils.get_glue_context()
    config = Config(ConfigType.S3, "ryozen-glue", "etl002/etl002.ini", None)
    Etl(context, config).run()
