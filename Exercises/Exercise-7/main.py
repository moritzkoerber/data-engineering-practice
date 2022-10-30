import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, SparkSession, Window
from utils.utils import (
    create_spark_dataframe_from_memory,
    read_zipfile_content_into_memory,
)


def add_ranking_column(column_to_rank: str, ranking_column: str) -> DataFrame:
    window = Window.orderBy(sf.col(column_to_rank).desc())

    def wrap(
        spark_data_frame: DataFrame,
    ):
        return spark_data_frame.drop_duplicates().withColumn(
            ranking_column, sf.dense_rank().over(window)
        )

    return wrap


def main():
    spark = (
        SparkSession.builder.appName("Exercise7")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )
    csv_file = read_zipfile_content_into_memory(
        "data/hard-drive-2022-01-01-failures.csv.zip"
    )[0]

    sdf = (
        create_spark_dataframe_from_memory(
            csv_file.decode("utf-8").splitlines(), spark, header=True
        )
        .withColumn("model", sf.regexp_replace("model", r"\s{2,}", r"\s"))
        .withColumn("capacity_bytes", sf.col("capacity_bytes").astype("long"))
    ).cache()

    sdf.withColumn(
        "brand",
        sf.when(
            sf.size(sf.split("model", " ")) > 1, sf.split("model", " ")[0]
        ).otherwise("unknown"),
    ).withColumn(
        "source_file", sf.lit("hard-drive-2022-01-01-failures.csv")
    ).withColumn(
        "file_date",
        sf.to_date(sf.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1)),
    ).join(
        sdf.select("model", "capacity_bytes")
        .transform(add_ranking_column("capacity_bytes", "storage_ranking"))
        .select("model", "storage_ranking"),
        on="model",
        how="left",
    ).withColumn(
        "primary_key", sf.hash("model", "serial_number")
    )


if __name__ == "__main__":
    main()
