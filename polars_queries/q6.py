from datetime import datetime

import polars as pl
from linetimer import CodeTimer, linetimer

from polars_queries import polars_tpch_utils

Q_NUM = 6


@linetimer(name=f"Overall execution of Query {Q_NUM}", unit="s")
def q():
    var1 = datetime(1994, 1, 1)
    var2 = datetime(1995, 1, 1)
    var3 = 24

    line_item_ds = polars_tpch_utils.get_line_item_ds()

    with CodeTimer(name=f"Get result of Query {Q_NUM}", unit="s"):
        result_df = (
            line_item_ds.filter(pl.col("l_shipdate") >= var1)
            .filter(pl.col("l_shipdate") < var2)
            .filter((pl.col("l_discount") >= 0.05) & (pl.col("l_discount") <= 0.07))
            .filter(pl.col("l_quantity") < var3)
            .with_column(
                (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
            )
            .select(pl.sum("revenue").alias("revenue"))
        ).collect()

        print(result_df)
        polars_tpch_utils.test_results(Q_NUM, result_df)


if __name__ == "__main__":
    q()
