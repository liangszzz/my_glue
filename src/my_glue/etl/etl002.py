from awsglue.context import DataFrame, GlueContext
from boto3 import client

from my_glue.common.base import Base
from my_glue.common.input_output_source import InputOutputWithConfig
from my_glue.utils import glue_utils, s3_utils


class Etl(Base):
    def __init__(self, context: GlueContext, s3: client, config: str = "src/my_glue/config/etl/etl002.ini") -> None:
        config = InputOutputWithConfig(context, s3, config)
        super().__init__(context, s3, config.get_job_config())

    def handle_data(self) -> DataFrame:
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
        return self.context.spark_session.sql(sql)


if __name__ == "__main__":
    Etl(glue_utils.get_glue_context(), s3_utils.get_client()).run()
