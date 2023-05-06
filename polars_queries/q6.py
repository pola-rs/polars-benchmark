from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 6


def q():
    var_1 = datetime(1994, 1, 1)
    var_2 = datetime(1995, 1, 1)
    var_3 = 24

    line_item_ds = utils.get_line_item_ds()

    q_final = (
        line_item_ds.filter(
            pl.col("l_shipdate").is_between(var_1, var_2, closed="left")
        )
        .filter(pl.col("l_discount").is_between(0.05, 0.07))
        .filter(pl.col("l_quantity") < var_3)
        .with_columns(
            (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
        )
        .select(pl.sum("revenue").alias("revenue"))
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
