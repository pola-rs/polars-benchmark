from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 6


def q() -> None:
    lineitem = utils.get_line_item_ds()

    var1 = date(1994, 1, 1)
    var2 = date(1995, 1, 1)
    var3 = 0.05
    var4 = 0.07
    var5 = 24

    q_final = (
        lineitem.filter(pl.col("l_shipdate").is_between(var1, var2, closed="left"))
        .filter(pl.col("l_discount").is_between(var3, var4))
        .filter(pl.col("l_quantity") < var5)
        .with_columns(
            (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
        )
        .select(pl.sum("revenue"))
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
