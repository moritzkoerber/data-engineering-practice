import re
from pathlib import Path
from zipfile import ZipFile

from awsume.awsumepy import awsume
from pyspark.sql import DataFrame, SparkSession


def get_assumed_role_boto3_session(profile_name):
    return awsume(profile_name)


def create_directory(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)


def read_zipfile_content_into_memory(zipfile_path: Path):
    with ZipFile(zipfile_path, "r") as zip:
        return [zip.read(x) for x in zip.namelist() if re.fullmatch(r"[^/]*\.csv", x)]


def extract_and_return_csv_filepaths(zip_file_path: Path):
    with ZipFile(zip_file_path, "r") as zip:
        return [
            zip.extract(e, path="data")
            for e in zip.namelist()
            if re.fullmatch(r"[^/]*\.csv", e)
        ]


def create_spark_dataframe_from_memory(
    list_str: list, sc: SparkSession, **kwargs
) -> DataFrame:
    p = sc.sparkContext.parallelize(list_str)
    return sc.read.csv(p, **kwargs)
