import pandas
import pytest
from main import add_ranking_column
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def create_spark_session():
    return (
        SparkSession.builder.appName("Exercise7Test").master("local[*]").getOrCreate()
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
                [("A", 1), ("B", 2), ("C", 3)],
                ["model", "capacity_bytes"],
            ],
            dict(
                model=["C", "B", "A"],
                capacity_bytes=[3, 2, 1],
                storage_ranking=[1, 2, 3],
            ),
        )
    ],
    indirect=True,
)
def test_ranking_function(create_spark_session, input_sdf, expected_df):
    output_df = input_sdf.transform(
        add_ranking_column("capacity_bytes", "storage_ranking")
    ).toPandas()
    assert_frame_equal(output_df, expected_df, check_dtype=False)
