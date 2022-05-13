import os
import time
from datetime import datetime

import polars as pl

os.environ["POLARS_VERBOSE"] = "1"

q = pl.scan_parquet(
    "tables_scale_1/lineitem.parquet",
)

q = (
    q.filter(pl.col("l_shipdate") <= datetime(1998, 12, 1))
    .groupby(["l_returnflag", "l_linestatus"])
    .agg(
        [
            pl.sum("l_quantity").alias("sum_qty"),
            pl.sum("l_extendedprice").alias("sum_base_price"),
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
            .sum()
            .alias("sum_disc_price"),
            (
                pl.col("l_extendedprice")
                * (1.0 - pl.col("l_discount") * (1.0 - pl.col("l_tax")))
            )
            .sum()
            .alias("sum_charge"),
            pl.mean("l_quantity").alias("avg_qty"),
            pl.mean("l_extendedprice").alias("avg_price"),
            pl.mean("l_discount").alias("avg_disc"),
            pl.count().alias("count_order"),
        ],
    )
    .sort(["l_returnflag", "l_linestatus"])
)

t0 = time.time()
print(q.collect())
print(time.time() - t0)
