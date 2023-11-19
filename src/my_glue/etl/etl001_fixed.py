from awsglue.context import GlueContext
from my_glue.common.base import Base
from my_glue.common.config import Config, ConfigType
from my_glue.utils import glue_utils
from pyspark.sql.functions import col, to_date, trim, regexp_replace, encode, substring, decode


class Etl(Base):
    def __init__(self, context: GlueContext, config: Config) -> None:
        super().__init__(context, config)
        self.charset = "shift-jis"

    def handler_args(self) -> None:
        super().handler_args()

    def load_data(self) -> None:
        self.defalut_fixed_options["encoding"] = self.charset
        df = self.load_s3_file(
            "input1",
            create_view_flag=False,
            format_map={
                "action_date": self.action_date,
                "input_file_type": self.args["input_file_type"],
                "input_file_bucket": self.args["input_file_bucket"],
                "input_file_path": self.args["input_file_path"],
            },
            optional_args=self.defalut_fixed_options,
        )
        df.show()
        df = df.withColumnRenamed("_c0", "value").withColumn("value", encode("value", self.charset))
        self.input_df = df

    def handle_data(self) -> None:
        df = self.input_df
        split_count = self.args["split_count"].split(",")
        split_name = self.args["split_name"].split(",")

        start = 0
        for i in range(len(split_count)):
            length = int(split_count[i])
            df = df.withColumn(split_name[i], decode(substring("value", start + 1, length), self.charset))
            start = start + length
        df = df.drop("value")

        decimal_columns = self.args["decimal_columns"].split(",")
        date_columns = self.args["date_columns"].split(",")

        columns = df.columns

        for column in columns:
            # a.trim
            df = df.withColumn(column, regexp_replace(trim(col(column)), "\\.$", ""))
            # b.decimal parse
            if column in decimal_columns:
                df = df.withColumn(column, col(column).cast("decimal(15,2)"))
            # c.date parse
            if column in date_columns:
                df = df.withColumn(column, to_date(col(column), self.args["date_fromat"]))
        self.export_df = df

    def export_data(self) -> None:
        self.export_to_s3(
            "output1",
            self.export_df,
            format_map={
                "action_date": self.action_date,
                "output_file_bucket": self.args["output_file_bucket"],
                "output_file_path": self.args["output_file_path"],
            },
            optional_args=self.defalut_parquet_options,
        )


if __name__ == "__main__":
    context = glue_utils.get_glue_context()
    config = Config(ConfigType.S3, "ryozen-glue", "etl001/etl001_fixed.ini", None)
    Etl(context, config).run()
