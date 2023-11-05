from awsglue.context import GlueContext

from my_glue.common.base import Base
from my_glue.common.config import Config, ConfigType
from my_glue.utils import glue_utils


class Etl(Base):
    def __init__(self, context: GlueContext, config: Config) -> None:
        super().__init__(context, config)

    def load_data(self) -> None:
        self.df1 = self.load_s3_file("input1", cache_flag=False, create_view_flag=False,
                                     prefix_map={"action_date": self.action_date})
        self.df2 = self.load_s3_file("input2", cache_flag=False, create_view_flag=False,
                                     prefix_map={"action_date": self.action_date})

    def handle_data(self) -> None:
        self.export_df = self.df1.join(self.df2, "address_code", "full")

    def export_data(self) -> None:
        self.export_to_s3("output1", self.export_df, prefix_map={"action_date": self.action_date})


if __name__ == "__main__":
    context = glue_utils.get_glue_context()
    config = Config(ConfigType.S3, "ryozen-glue", "etl002/etl003.ini", None)
    Etl(context, config).run()
