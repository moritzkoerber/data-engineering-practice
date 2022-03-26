import logging
import os
import re
from argparse import ArgumentParser
from pathlib import Path
from zipfile import ZipFile

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, SparkSession, Window
from utils.utils import create_directory

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

parser = ArgumentParser()
parser.add_argument("--in_memory", action="store_true")

directory = "reports"


def create_spark_dataframe_from_memory(
    list_str: list, sc: SparkSession, **kwargs
) -> DataFrame:
    p = sc.sparkContext.parallelize(list_str)
    return sc.read.csv(p, **kwargs)


def read_zipfile_content_to_memory(zipfile_path: Path):
    with ZipFile(zipfile_path, "r") as zip:
        return [zip.read(x) for x in zip.namelist() if re.fullmatch(r"[^/]*\.csv", x)]


def extract_and_return_csv_filepaths(zip_file_path: Path):
    with ZipFile(zip_file_path, "r") as zip:
        return [
            zip.extract(e, path="data")
            for e in zip.namelist()
            if re.fullmatch(r"[^/]*\.csv", e)
        ]


def read_data_into_spark(zip_file_paths: list[Path], sc: SparkSession):
    csv_frames = {}

    for zip_file_path in zip_file_paths:
        if not parser.parse_args().in_memory:
            logger.info(f"Reading files in {zip_file_path} from disk")
            zip_contents = extract_and_return_csv_filepaths(zip_file_path)
            for csv_file in zip_contents:
                temp_sdf = sc.read.csv(csv_file, header=True)
                csv_frames[csv_file] = temp_sdf.withColumn(
                    "date",
                    sf.to_date(*({"started_at", "start_time"} & set(temp_sdf.columns))),
                ).cache()
            continue

        zip_contents = [e for e in read_zipfile_content_to_memory(zip_file_path)]
        for csv_file in zip_contents:
            logger.info(f"Reading files in {zip_file_path} from memory")

            temp_sdf = create_spark_dataframe_from_memory(
                csv_file.decode("utf-8").splitlines(), sc, header=True
            )
            csv_frames[csv_file] = temp_sdf.withColumn(
                "date",
                sf.to_date(*({"started_at", "start_time"} & set(temp_sdf.columns))),
            ).cache()
    return csv_frames


class QuestionAnsweringService:
    def __init__(self, sdf: DataFrame, data_name: str, sc: SparkSession):
        self.sdf = sdf
        self.data_name = data_name
        self.sc = sc

    def answer_question1(self, file_path: Path):
        """
        1. What is the `average` trip duration per day?
        """
        logging.info("Answering question 1")
        sdf = self.sdf
        if self.data_name == "data/Divvy_Trips_2020_Q1.csv":
            sdf = sdf.select(
                "*",
                (
                    sf.unix_timestamp(sf.to_timestamp("ended_at"))
                    - sf.unix_timestamp(sf.to_timestamp("started_at"))
                ).alias("tripduration"),
            )
        result_sdf = sdf.groupby("date").agg(
            sf.mean("tripduration").alias("average_trip_duration")
        )

        if file_path.is_dir():
            result_sdf.repartition(1).write.mode("append").csv(
                os.fspath(file_path), header=True
            )
            return
        result_sdf.repartition(1).write.mode("overwrite").csv(
            os.fspath(file_path), header=True
        )

    def answer_question2(self, file_path: Path):
        """
        2. How many trips were taken each day?
        """
        logging.info("Answering question 2")

        result_sdf = self.sdf.groupby("date").count()

        if file_path.is_dir():
            result_sdf_existing = self.sc.read.csv(os.fspath(file_path), header=True)
            result_sdf_existing.cache().count()
            result_sdf.cache().count()
            result_sdf_existing.unionByName(result_sdf).repartition(1).write.mode(
                "overwrite"
            ).csv(os.fspath(file_path), header=True)
            return
        result_sdf.repartition(1).write.mode("overwrite").csv(
            os.fspath(file_path), header=True
        )

    def answer_question3(self, file_path: Path):
        """
        3. What was the most popular starting trip station for each month?
        """
        logging.info("Answering question 3")

        result_sdf = (
            self.sdf.select(
                "*",
                *[
                    func(sf.col(x)).alias(y)
                    for x, y, func in zip(
                        ["date", "date"], ["year", "month"], [sf.year, sf.month]
                    )
                ],
                sf.col(
                    *{"start_station_id", "from_station_id"} & set(self.sdf.columns)
                ).alias("start_location"),
            )
            .groupby("year", "month", "start_location")
            .count()
            .orderBy(["year", "month", "count"], ascending=False)
            .groupby("year", "month")
            .agg(sf.first("start_location").alias("most_popular_start_location"))
        )

        if file_path.is_dir():
            result_sdf_existing = self.sc.read.csv(os.fspath(file_path), header=True)
            result_sdf_existing.cache().count()
            result_sdf.cache().count()
            result_sdf_existing.unionByName(result_sdf).repartition(1).write.mode(
                "overwrite"
            ).csv(os.fspath(file_path), header=True)
            return

        result_sdf.repartition(1).write.mode("overwrite").csv(
            os.fspath(file_path), header=True
        )

    def answer_question4(self, file_path: Path):
        """
        4. What were the top 3 trip stations each day for the last two weeks?
        """
        logging.info("Answering question 4")

        if self.data_name == "data/Divvy_Trips_2020_Q1.csv":
            window = Window.partitionBy("date").orderBy("count")
            date_cutoff = self.sdf.select(
                sf.date_add(sf.max("date"), -14).alias("max_date")
            ).collect()[0]["max_date"]

            self.sdf.where(sf.col("date") >= date_cutoff).groupby(
                "date", "start_station_name"
            ).count().withColumn("rank", sf.row_number().over(window)).where(
                sf.col("rank") <= 3
            ).write.mode(
                "overwrite"
            ).csv(
                os.fspath(file_path), header=True
            )

    def answer_question5(self, file_path: Path):
        """
        5. Do `Male`s or `Female`s take longer trips on average?
        """
        logging.info("Answering question 5")

        if self.data_name == "data/Divvy_Trips_2019_Q4.csv":
            self.sdf.dropna(subset=["tripduration", "gender"]).groupby("gender").agg(
                sf.mean("tripduration").alias("avg_tripduration")
            ).selectExpr(
                "max_by(gender, avg_tripduration) as longest_trip_takers"
            ).repartition(
                1
            ).write.mode(
                "overwrite"
            ).csv(
                os.fspath(file_path), header=True
            )

    def answer_question6(self, file_path: Path):
        """
        6. What is the top 10 ages of those that take the longest trips, and shortest
        """
        logging.info("Answering question 6")
        if self.data_name == "data/Divvy_Trips_2019_Q4.csv":
            self.sdf.orderBy(sf.desc("tripduration")).dropna(
                subset=["birthyear"]
            ).limit(10).select((2022 - sf.col("birthyear")).alias("age")).repartition(
                1
            ).write.mode(
                "overwrite"
            ).csv(
                os.fspath(file_path), header=True
            )


def main():
    sc = SparkSession.builder.appName("Exercise6").getOrCreate()

    create_directory(directory)

    zip_file_paths = list(Path().rglob("*.zip"))

    csv_frames = read_data_into_spark(zip_file_paths, sc)

    for sdf, data_name in zip(csv_frames.values(), csv_frames.keys()):
        qas = QuestionAnsweringService(sdf, data_name, sc)
        qas.answer_question1(Path(f"{directory}/question1.csv"))
        qas.answer_question2(Path(f"{directory}/question2.csv"))
        qas.answer_question3(Path(f"{directory}/question3.csv"))
        qas.answer_question4(Path(f"{directory}/question4.csv"))
        qas.answer_question5(Path(f"{directory}/question5.csv"))
        qas.answer_question6(Path(f"{directory}/question6.csv"))


if __name__ == "__main__":
    main()
