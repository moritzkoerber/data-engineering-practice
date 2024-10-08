import asyncio
import io
import zipfile
from argparse import ArgumentParser
from pathlib import Path

import aiohttp
import requests
from requests.exceptions import HTTPError
from utils.utils import create_directory

parser = ArgumentParser()
parser.add_argument("--call_async", action="store_true")


def download_and_unzip(
    session: requests.Session, uri: str, download_directory: str | Path
):
    with session.get(uri, stream=True, timeout=60) as response:
        try:
            response.raise_for_status()
            zipfile.ZipFile(io.BytesIO(response.content)).extractall(download_directory)
        except HTTPError as h:
            print(f"Invalid request for {uri}")
            print(h)


def slow_main(download_uris: list[str], download_directory: str | Path):
    with requests.Session() as s:
        for uri in download_uris:
            download_and_unzip(s, uri, download_directory)


async def async_download_and_unzip(
    session: aiohttp.ClientSession, uri: str, download_directory: str | Path
):
    async with session.get(uri, timeout=60) as response:
        try:
            response.raise_for_status()
            content = await response.read()
            zipfile.ZipFile(io.BytesIO(content)).extractall(download_directory)
        except aiohttp.ClientResponseError as c:
            print(f"Invalid request for {uri}")
            print(c)


async def async_main(download_uris, download_directory):
    async with aiohttp.ClientSession() as s:
        await asyncio.gather(
            *[
                async_download_and_unzip(s, uri, download_directory)
                for uri in download_uris
            ],
            return_exceptions=True,
        )


def main():
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

    create_directory(download_directory)

    if parser.parse_args().call_async:
        print("Running tasks concurrently:")
        asyncio.run(async_main(download_uris, download_directory))
    else:
        print("Running tasks strictly sequentially:")
        slow_main(download_uris, download_directory)


if __name__ == "__main__":
    main()
