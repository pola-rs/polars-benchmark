import polars as pl

web_sales = pl.scan_parquet("data/web_sales.parquet")
catalog_sales = pl.scan_parquet("data/catalog_sales.parquet")
date_dim = pl.scan_parquet("data/date_dim.parquet")


wscs = pl.concat(
    [
        (
            web_sales.select(
                [
                    pl.col("ws_sold_date_sk").alias("sold_date_sk"),
                    pl.col("ws_ext_sales_price").alias("sales_price"),
                ]
            )
        ),
        (
            catalog_sales.select(
                [
                    pl.col("cs_sold_date_sk").alias("sold_date_sk"),
                    pl.col("cs_ext_sales_price").alias("sales_price"),
                ]
            )
        ),
    ]
)

wswcs = (
    wscs.join(date_dim, left_on="sold_date_sk", right_on="d_date_sk")
    .group_by("d_week_seq")
    .agg(
        [
            pl.col("sales_price")
            .filter(pl.col("d_day_name") == "Sunday")
            .sum()
            .alias("sun_sales"),
            pl.col("sales_price")
            .filter(pl.col("d_day_name") == "Monday")
            .sum()
            .alias("mon_sales"),
            pl.col("sales_price")
            .filter(pl.col("d_day_name") == "Tuesday")
            .sum()
            .alias("tue_sales"),
            pl.col("sales_price")
            .filter(pl.col("d_day_name") == "Wednesday")
            .sum()
            .alias("wed_sales"),
            pl.col("sales_price")
            .filter(pl.col("d_day_name") == "Thursday")
            .sum()
            .alias("thu_sales"),
            pl.col("sales_price")
            .filter(pl.col("d_day_name") == "Friday")
            .sum()
            .alias("fri_sales"),
            pl.col("sales_price")
            .filter(pl.col("d_day_name") == "Saturday")
            .sum()
            .alias("sat_sales"),
        ]
    )
)

y = (
    wswcs.join(
        date_dim.select([pl.col("d_week_seq"), pl.col("d_year")]),
        on="d_week_seq",
    )
    .filter(pl.col("d_year") == 2001)
    .select(
        [
            pl.col("d_week_seq").alias("d_week_seq1"),
            pl.col("sun_sales").alias("sun_sales1"),
            pl.col("mon_sales").alias("mon_sales1"),
            pl.col("tue_sales").alias("tue_sales1"),
            pl.col("wed_sales").alias("wed_sales1"),
            pl.col("thu_sales").alias("thu_sales1"),
            pl.col("fri_sales").alias("fri_sales1"),
            pl.col("sat_sales").alias("sat_sales1"),
        ]
    )
)

z = (
    wswcs.join(
        date_dim.select([pl.col("d_week_seq"), pl.col("d_year")]),
        on="d_week_seq",
    )
    .filter(pl.col("d_year") == 2002)
    .select(
        [
            pl.col("d_week_seq").alias("d_week_seq2"),
            pl.col("sun_sales").alias("sun_sales2"),
            pl.col("mon_sales").alias("mon_sales2"),
            pl.col("tue_sales").alias("tue_sales2"),
            pl.col("wed_sales").alias("wed_sales2"),
            pl.col("thu_sales").alias("thu_sales2"),
            pl.col("fri_sales").alias("fri_sales2"),
            pl.col("sat_sales").alias("sat_sales2"),
        ]
    )
)


y.join(
    z.with_columns([(pl.col("d_week_seq2") - 53).alias("key")]),
    left_on="d_week_seq1",
    right_on="key",
).select(
    [
        pl.col("d_week_seq1"),
        (pl.col("sun_sales1") / pl.col("sun_sales2"))
        .cast(pl.Float64)
        .round(2)
        .alias("r1"),
        (pl.col("mon_sales1") / pl.col("mon_sales2"))
        .cast(pl.Float64)
        .round(2)
        .alias("r2"),
        (pl.col("tue_sales1") / pl.col("tue_sales2"))
        .cast(pl.Float64)
        .round(2)
        .alias("r3"),
        (pl.col("wed_sales1") / pl.col("wed_sales2"))
        .cast(pl.Float64)
        .round(2)
        .alias("r4"),
        (pl.col("thu_sales1") / pl.col("thu_sales2"))
        .cast(pl.Float64)
        .round(2)
        .alias("r5"),
        (pl.col("fri_sales1") / pl.col("fri_sales2"))
        .cast(pl.Float64)
        .round(2)
        .alias("r6"),
        (pl.col("sat_sales1") / pl.col("sat_sales2"))
        .cast(pl.Float64)
        .round(2)
        .alias("r7"),
    ]
).sort(by="d_week_seq1", nulls_last=False).collect(engine="streaming")
