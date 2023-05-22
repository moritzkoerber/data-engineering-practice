import re
from pathlib import Path
from zipfile import ZipFile

from awsume.awsumepy import awsume
from pyspark.sql import DataFrame, SparkSession


def get_assumed_role_boto3_session(profile_name: str):
    return awsume(profile_name)


def create_directory(directory: str):
    Path(directory).mkdir(parents=True, exist_ok=True)


def read_zipfile_content_into_memory(zipfile_path: Path) -> list:
    with ZipFile(zipfile_path, "r") as zip_file:
        return [
            zip_file.read(x)
            for x in zip_file.namelist()
            if re.fullmatch(r"[^/]*\.csv", x)
        ]


def extract_and_return_csv_filepaths(zip_file_path: Path) -> list:
    with ZipFile(zip_file_path, "r") as zip_file:
        return [
            zip_file.extract(e, path="data")
            for e in zip_file.namelist()
            if re.fullmatch(r"[^/]*\.csv", e)
        ]


def create_spark_dataframe_from_memory(
    list_str: list, sc: SparkSession, **kwargs
) -> DataFrame:
    p = sc.sparkContext.parallelize(list_str)
    return sc.read.csv(p, **kwargs)
