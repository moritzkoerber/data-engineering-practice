import pytest
from main import find_filenames_bs4, find_filenames_regex


@pytest.mark.parametrize(
    "url", ["https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"]
)
def test_regex_bs4_same_results(url):
    assert list(find_filenames_bs4(url)) == list(find_filenames_regex(url))
