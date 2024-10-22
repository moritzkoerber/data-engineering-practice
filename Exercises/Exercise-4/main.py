import csv
import json
from pathlib import Path


def flatten_json(path_to_json: str | Path) -> dict[str, str | bool | int | float]:
    output_dict = {}

    def _flatten(k, v):
        if isinstance(v, dict):
            for i, j in v.items():
                _flatten(i, j)
            return
        if isinstance(v, list):
            output_dict.update({f"{k}_{num}": el for num, el in enumerate(v)})
            return
        output_dict[k] = v

    with open(path_to_json, "r") as f:
        for k, v in json.load(f).items():
            _flatten(k, v)
    return output_dict  # noqa


def write_csv_file(dict_list: list[dict], path: str | Path):
    with open(path, "w") as f:
        writer = csv.DictWriter(f, dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)


def main():
    file_list = list(Path("data").rglob("*.json"))
    dict_list = [flatten_json(e) for e in file_list]
    if all(e.keys() == dict_list[0].keys() for e in dict_list):
        write_csv_file(dict_list, Path(__file__).parent / "output.csv")
    else:
        raise Exception("JSON files have different keys")


if __name__ == "__main__":
    main()
