from awsglue.context import DataFrame, GlueContext
from pyspark.context import SparkContext

from my_glue.common.base import Base
from my_glue.common.input_output_source import InputOutputWithConfig
from my_glue.utils import s3_utils


class Etl001(Base):
    def __init__(self, context: GlueContext) -> None:
        s3 = s3_utils.get_client()
        config = InputOutputWithConfig(context, s3, "src/my_glue/config/etl/etl001.ini")
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
    Etl001(GlueContext(SparkContext())).run()
