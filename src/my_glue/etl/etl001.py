from awsglue.context import DataFrame, GlueContext
from boto3 import client as s3_client
from pyspark.context import SparkContext

from my_glue.common.base import Base, JobConfig
from my_glue.common.input_output import InputOutputWithConfig


class Etl(Base):
    def __init__(self, context: GlueContext, s3: s3_client, config: JobConfig) -> None:
        super().__init__(context, s3, config)

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
    glue_context = GlueContext(SparkContext())
    s3 = s3_client("s3")
    config = InputOutputWithConfig(glue_context, s3, "src/my_glue/config/etl/etl001.ini")
    Etl(glue_context, s3, config.get_job_config()).run()
