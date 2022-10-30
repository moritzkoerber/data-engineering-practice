from pathlib import Path
from tempfile import TemporaryDirectory

import pandas
import pytest
from main import QuestionAnsweringService
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def create_spark_session():
    return (
        SparkSession.builder.appName("Exercise6Test").master("local[*]").getOrCreate()
    )


@pytest.fixture(scope="function")
def input_sdf(create_spark_session, request):
    return create_spark_session.createDataFrame(*request.param)


@pytest.fixture(scope="function")
def expected_df(request):
    return pandas.DataFrame(request.param)


@pytest.mark.parametrize(
    ("input_sdf, expected_df"),
    [
        (
            [
                [("male", 3600), ("male", 4000), ("female", 1800)],
                ["gender", "tripduration"],
            ],
            dict(longest_trip_takers=["male"]),
        )
    ],
    indirect=True,
)
def test_answer_question5(create_spark_session, input_sdf, expected_df):
    qas = QuestionAnsweringService(
        input_sdf, "data/Divvy_Trips_2019_Q4.csv", create_spark_session
    )
    with TemporaryDirectory() as tmpdirname:
        print("created temporary directory", tmpdirname)
        qas.answer_question5(f"{tmpdirname}/question5.csv")
        write_path = Path(f"{tmpdirname}/question5.csv/").glob("*.csv")

        assert_frame_equal(pandas.read_csv(list(write_path)[0]), expected_df)
