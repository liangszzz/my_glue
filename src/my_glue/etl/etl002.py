from awsglue.context import DataFrame, GlueContext
from boto3 import client
from pyspark.context import SparkContext

from my_glue.common.base import Base, JobConfig
from my_glue.common.input_output_source import InputOutputWithConfig


class Etl(Base):
    def __init__(self, context: GlueContext, s3: client, config: JobConfig) -> None:
        super().__init__(context, s3, config)

    def handle_data(self) -> DataFrame:
        sql = """
            SELECT
                *
            FROM output_view
            """
        df = self.context.spark_session.sql(sql)
        df.show()

        return df


if __name__ == "__main__":
    glue_context = GlueContext(SparkContext())
    s3 = client("s3")
    config = InputOutputWithConfig(glue_context, s3, "src/my_glue/config/etl/etl002.ini")
    Etl(glue_context, s3, config.get_job_config()).run()
