import argparse
import io

import pandas
import requests
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()
parser.add_argument("mode", type=str)

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def find_filenames_bs4(url: str) -> list:
    with requests.get(url, timeout=60) as response:
        table = BeautifulSoup(response.content, "html.parser").find("table")
        rows = table.find_all("tr")
        return [
            f"{url}{f.previous_sibling.string}"
            for td in rows
            if (f := td.find("td", string="2022-02-07 14:03  "))
        ]


def find_filenames_regex(url: str) -> list:
    import re

    with requests.get(url, timeout=16) as response:
        pattern = re.compile(r"([A-z0-9]{11}\.csv)")

        s = [
            pattern.search(i)
            for i in response.text.splitlines()
            if re.search(r"2022-02-07 14:03\s{2}", i)
        ]

        return [f"{url}{i.group(0)}" for i in s if i]


def download_and_print(url_list: list) -> None:
    for e in url_list:
        res = requests.get(e, timeout=60)

        df = pandas.read_csv(io.BytesIO(res.content)).assign(
            HourlyDryBulbTemperature=lambda x: pandas.to_numeric(
                x.HourlyDryBulbTemperature, errors="coerce"
            )
        )
        print(df.loc[df.HourlyDryBulbTemperature.idxmax()])


def main():
    if (mode := parser.parse_args().mode) == "beautifulsoup":
        url_list = find_filenames_bs4(url)
    elif mode == "regex":
        url_list = find_filenames_regex(url)
    else:
        raise KeyError("Please specify either 'beautifulsoup' or 'regex'")

    download_and_print(url_list)


if __name__ == "__main__":
    main()
