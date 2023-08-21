import polars as pl


def read_data(path: str) -> pl.DataFrame:
    return pl.read_csv(
        path,
        columns=["started_at"],
        dtypes={
            "ride_id": pl.Utf8,
            "rideable_type": pl.Categorical,
            "started_at": pl.Datetime,
            "ended_at": pl.Datetime,
            "start_station_name": pl.Utf8,
            "start_station_id": pl.Int32,
            "end_station_name": pl.Utf8,
            "end_station_id": pl.Int32,
            "start_lat": pl.Float32,
            "start_lng": pl.Float32,
            "end_lat": pl.Float32,
            "end_lng": pl.Float32,
            "member_casual": pl.Categorical,
        },
    )


def rides_per_day(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("started_at").dt.date().alias("started_at_date"))
        .groupby("started_at_date")
        .count()
    )


def aggregates_per_week(df: pl.DataFrame) -> pl.DataFrame:
    df_agg = (
        df.with_columns(pl.col("started_at").dt.week())
        .groupby("started_at")
        .agg(cnt=pl.col("started_at").count())
    )
    return df_agg.select(
        pl.mean("cnt").alias("mean"),
        pl.min("cnt").alias("min"),
        pl.max("cnt").alias("max"),
    )


def diff_to_last_week(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.sort("started_at_date")
        .with_columns(
            pl.col("count").shift(7).alias("count_last_week"),
        )
        .select(
            pl.col("count"),
            pl.col("count_last_week"),
            (pl.col("count") - pl.col("count_last_week")).alias("diff_to_last_week"),
        )
    )


def main():
    df = read_data("data/202306-divvy-tripdata.csv")
    rides_per_day_df = rides_per_day(df)
    print(rides_per_day_df)
    print(aggregates_per_week(df))
    print(
        diff_to_last_week(rides_per_day_df).with_columns(pl.col("count").cast(pl.Int32))
    )


if __name__ == "__main__":
    main()
