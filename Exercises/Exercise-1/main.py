import asyncio
import io
import zipfile
from argparse import ArgumentParser
from pathlib import Path

import requests
from requests.exceptions import HTTPError
from utils.utils import create_directory

parser = ArgumentParser()
parser.add_argument("--call_async", action="store_true")

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

download_directory = Path(__file__).parent / "downloads"


def download_and_unzip(uri: str, download_directory: Path):
    with requests.Session() as s, s.get(uri, stream=True, timeout=60) as response:
        try:
            response.raise_for_status()
            zipfile.ZipFile(io.BytesIO(response.content)).extractall(download_directory)
        except HTTPError as h:
            print(f"Invalid request for {uri}")
            print(h)


def main():
    for uri in download_uris:
        download_and_unzip(uri, download_directory)


async def async_download_and_unzip(uri: str, download_directory: str):
    with requests.Session() as s, s.get(uri, stream=True, timeout=60) as response:
        try:
            response.raise_for_status()
            zipfile.ZipFile(io.BytesIO(response.content)).extractall(download_directory)
        except HTTPError as h:
            print(f"Invalid request for {uri}")
            print(h)


async def async_main():
    for uri in download_uris:
        await async_download_and_unzip(uri, download_directory)


if __name__ == "__main__":
    create_directory(download_directory)
    if parser.parse_args().call_async:
        asyncio.run(async_main())
    else:
        main()
