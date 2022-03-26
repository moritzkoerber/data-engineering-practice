import argparse
import io

import pandas
import requests
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()
parser.add_argument("mode", type=str)

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def find_filenames_bs4(url):
    with requests.get(url) as response:
        table = BeautifulSoup(response.content, "html.parser").find("table")
        rows = table.find_all("tr")
        url_list = [
            f"{url}{f.previous_sibling.string}"
            for td in rows
            if (f := td.find("td", string="2022-02-07 14:03  "))
        ]
        return url_list


def find_filenames_regex(url):
    import re

    with requests.get(url) as response:
        pattern = re.compile(r"([A-z0-9]{11}\.csv)")

        s = [
            pattern.search(i)
            for i in response.text.splitlines()
            if re.search(r"2022-02-07 14:03  ", i)
        ]

        url_list = [f"{url}{i.group(0)}" for i in s if i]
        return url_list


def download_and_print(url_list):
    for e in url_list:
        res = requests.get(e)

        df = pandas.read_csv(io.BytesIO(res.content))
        print(df.loc[df.HourlyDryBulbTemperature.idxmax()])


def main():
    url_list = {
        "beautifulsoup": find_filenames_bs4(url),
        "regex": find_filenames_regex(url),
    }.get(
        parser.parse_args().mode,
        KeyError("Please specify either 'beautifulsoup' or 'regex'"),
    )

    download_and_print(url_list)


if __name__ == "__main__":
    main()
