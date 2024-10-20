import asyncio
import io
import logging
import zipfile
from argparse import ArgumentParser
from pathlib import Path

import aiohttp
import requests
from requests.exceptions import HTTPError
from utils.utils import create_directory


def unzip_csv_content(content: bytes, download_directory: str | Path):
    with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
        zip_file.extractall(download_directory)


def download_csv_bytes(session: requests.Session, uri: str) -> bytes | None:
    with session.get(uri, stream=True, timeout=60) as response:
        try:
            response.raise_for_status()
            return response.content
        except HTTPError as h:
            logging.error("Invalid request for %s", uri)
            logging.error(h)


def slow_main(download_uris: list[str], download_directory: str | Path):
    with requests.Session() as s:
        for uri in download_uris:
            if content := download_csv_bytes(s, uri):
                unzip_csv_content(content, download_directory)


async def async_download_and_unzip(
    session: aiohttp.ClientSession, uri: str, download_directory: str | Path
):
    async with session.get(uri, timeout=60) as response:
        try:
            response.raise_for_status()
            content = await response.read()
            await asyncio.to_thread(unzip_csv_content, content, download_directory)
        except aiohttp.ClientResponseError as c:
            logging.error("Invalid request for %s", uri)
            logging.error(c)


async def async_main(download_uris: list[str], download_directory: str | Path):
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

    parser = ArgumentParser()
    parser.add_argument("--call_async", action="store_true")

    if parser.parse_args().call_async:
        logging.info("Running tasks concurrently:")
        asyncio.run(async_main(download_uris, download_directory))
    else:
        logging.info("Running tasks strictly sequentially:")
        slow_main(download_uris, download_directory)


if __name__ == "__main__":
    main()
