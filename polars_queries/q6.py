from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 6


def q():
    var1 = datetime(1994, 1, 1)
    var2 = datetime(1995, 1, 1)
    var3 = 24

    line_item_ds = utils.get_line_item_ds()

    q_final = (
        line_item_ds.filter(pl.col("l_shipdate") >= var1)
        .filter(pl.col("l_shipdate") < var2)
        .filter((pl.col("l_discount") >= 0.05) & (pl.col("l_discount") <= 0.07))
        .filter(pl.col("l_quantity") < var3)
        .with_column(
            (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
        )
        .select(pl.sum("revenue").alias("revenue"))
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
