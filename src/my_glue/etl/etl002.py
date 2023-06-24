from awsglue.context import DataFrame, GlueContext
from boto3 import client
from pyspark.context import SparkContext

from my_glue.common.base import Base, JobConfig
from my_glue.common.input_source import InputS3FileSource
from my_glue.common.output_source import OutputFile


class Etl(Base):
    def __init__(self, context: GlueContext, s3: client, config: JobConfig) -> None:
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
    s3 = client("s3")
    config = JobConfig(
        input_source=[
            InputS3FileSource(
                glue_context,
                s3,
                "ryozen-glue",
                "pysql/user2.csv",
                "user_view",
                required=False,
            ),
            InputS3FileSource(glue_context, s3, "ryozen-glue", "pysql/address.csv", "address_view"),
        ],
        output_source=OutputFile(glue_context, s3, "ryozen-glue", "pysql/output"),
    )
    Etl(glue_context, s3, config).run()
