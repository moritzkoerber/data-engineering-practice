# I added only tests for answer 1 as they all follow the same principle

import dataclasses
import io

import duckdb
import pandas
import pytest
from main import TableConfiguration, answer_question1, create_table_from_csv, table_cfg
from pandas.testing import assert_frame_equal
from soda.scan import Scan


@pytest.fixture(scope="module")
def input_df(request):
    if isinstance(request.param, pandas.DataFrame):
        return request.param
    return pandas.DataFrame(columns=request.param.keys()).astype(request.param)


@pytest.fixture(scope="function")
def create_temp_table_from_dataframe(request, input_df):
    file_obj = io.BytesIO()

    input_df.to_csv(file_obj, index=False)
    file_obj.seek(0)

    temp_table_cfg = dataclasses.replace(request.param, path=file_obj)

    create_table_from_csv(temp_table_cfg)

    yield input_df
    duckdb.sql(f"drop table if exists {temp_table_cfg.table_name}")


@pytest.mark.parametrize(
    "input_df, create_temp_table_from_dataframe",
    [
        (
            {
                "VIN (1-10)": "object",
                "County": "object",
                "City": "object",
                "State": "object",
                "Postal Code": "int",
                "Model Year": "int",
                "Make": "object",
                "Model": "object",
                "Electric Vehicle Type": "object",
                "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "object",
                "Electric Range": "int",
                "Base MSRP": "int",
                "Legislative District": "int",
                "DOL Vehicle ID": "int",
                "Vehicle Location": "object",
                "Electric Utility": "object",
                "2020 Census Tract": "int",
            },
            table_cfg,
        )
    ],
    indirect=True,
)
def test_create_table(input_df, create_temp_table_from_dataframe):
    result_df = duckdb.sql("select * from ev_population").to_df()
    assert_frame_equal(input_df, result_df, check_index_type=False)


@pytest.mark.parametrize(
    "input_df, create_temp_table_from_dataframe, expected_df",
    [
        (
            pandas.DataFrame({"City": ["New York", "New York", "Cincinnati"]}),
            TableConfiguration(
                path="",
                table_name="ev_population",
                schema={"City": "VARCHAR"},
                kwargs={"header": True},
            ),
            pandas.DataFrame({"City": ["New York", "Cincinnati"], "cnt": [2, 1]}),
        )
    ],
    indirect=["input_df", "create_temp_table_from_dataframe"],
)
def test_answer_question1(input_df, create_temp_table_from_dataframe, expected_df):
    assert_frame_equal(duckdb.sql(answer_question1()).to_df(), expected_df)


def test_answer_question1_soda():
    """
    This test uses Soda Core as an additional data quality check
    """
    with duckdb.connect(":memory:") as con:
        create_table_from_csv(table_cfg, con)
        con.sql(f"CREATE VIEW IF NOT EXISTS answer1 AS {answer_question1()}")
        scan = Scan()
        scan.set_verbose()
        scan.add_duckdb_connection(con)
        # scan.add_configuration_yaml_file("soda-sql/configuration.yml")
        scan.set_data_source_name("duckdb")
        scan.add_sodacl_yaml_files("soda-sql/checks.yml")
        scan.set_scan_definition_name("answer1")
        scan.execute()
        scan.assert_no_checks_fail()
