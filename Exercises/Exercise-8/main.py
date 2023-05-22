from dataclasses import dataclass, field

import duckdb


@dataclass
class TableConfiguration:
    path: str
    table_name: str
    schema: dict
    kwargs: dict = field(default_factory=dict)


table_cfg = TableConfiguration(
    path="data/Electric_Vehicle_Population_Data.csv",
    table_name="ev_population",
    schema={
        "VIN (1-10)": "VARCHAR",
        "County": "VARCHAR",
        "City": "VARCHAR",
        "State": "VARCHAR",
        "Postal Code": "BIGINT",
        "Model Year": "BIGINT",
        "Make": "VARCHAR",
        "Model": "VARCHAR",
        "Electric Vehicle Type": "VARCHAR",
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "VARCHAR",
        "Electric Range": "BIGINT",
        "Base MSRP": "BIGINT",
        "Legislative District": "BIGINT",
        "DOL Vehicle ID": "BIGINT",
        "Vehicle Location": "VARCHAR",
        "Electric Utility": "VARCHAR",
        "2020 Census Tract": "BIGINT",
    },
    kwargs={
        "header": True,
    },
)


def create_table_from_csv(
    table_cfg: TableConfiguration, connection: duckdb.DuckDBPyConnection = duckdb
):
    csv_content = connection.read_csv(  # noqa
        table_cfg.path, dtype=table_cfg.schema, **table_cfg.kwargs
    )
    connection.sql(f"CREATE TABLE {table_cfg.table_name} AS SELECT * FROM csv_content")


def answer_question1() -> str:
    return "SELECT CITY, COUNT(*) AS cnt FROM ev_population GROUP BY City"


def answer_question2() -> str:
    return """\
    SELECT Model, COUNT(*) as cnt
    FROM ev_population
    GROUP BY Model
    ORDER BY cnt DESC
    LIMIT 3
    """


def answer_question3() -> str:
    return """\
    WITH max_cnts as (
        SELECT
            "Postal Code",
            Model,
            COUNT(*) as cnt
        FROM ev_population
        GROUP BY "Postal Code", Model
        )

    SELECT
        "Postal Code",
        Model
    FROM
    (
        SELECT
            "Postal Code",
            Model,
            RANK() OVER (PARTITION BY "Postal Code" ORDER BY cnt desc) as rnk
        FROM max_cnts
    )
    WHERE rnk = 1
    """


def main():
    create_table_from_csv(table_cfg)
    for query in [answer_question1(), answer_question2(), answer_question3()]:
        duckdb.sql(query).show()


if __name__ == "__main__":
    main()
