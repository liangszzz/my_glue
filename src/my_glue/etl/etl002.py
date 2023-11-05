from awsglue.context import GlueContext
from boto3 import client

from my_glue.common.base import Base
from my_glue.common.config import Config, ConfigType
from my_glue.utils import glue_utils, s3_utils


class Etl(Base):
    def __init__(self, context: GlueContext, config: Config) -> None:
        super().__init__(context, config)

    def load_data(self) -> None:
        self.load_s3_file("input1")
        self.load_s3_file("input2")

    def handle_data(self) -> None:
        sql = """
                SELECT
                    user_view.id,
                    user_view.name,
                    user_view.age,
                    user_view.birthday,
                    concat(YEAR(user_view.birthday),LPAD(MONTH(user_view.birthday), 2, 0)) AS year_month,
                    concat(
                        address_view.key1,
                        '-',
                        address_view.key2,
                        '-',
                        address_view.key3
                    ) AS address
                FROM
                    user_view
                    JOIN address_view ON user_view.address_code = address_view.address_code
            """
        self.export_df = self.spark.sql(sql)

    def export_data(self) -> None:
        self.export_to_s3("output1", self.export_df)


if __name__ == "__main__":
    context = glue_utils.get_glue_context()
    config = Config(ConfigType.S3, "ryozen-glue", "etl002/etl002.ini", None)
    Etl(context, config).run()
