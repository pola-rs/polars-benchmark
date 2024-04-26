from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 4


def q() -> None:
    lineitem = utils.get_line_item_ds()
    orders = utils.get_orders_ds()

    var1 = date(1993, 7, 1)
    var2 = date(1993, 10, 1)

    q_final = (
        lineitem.join(orders, left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("o_orderdate").is_between(var1, var2, closed="left"))
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .unique(subset=["o_orderpriority", "l_orderkey"])
        .group_by("o_orderpriority")
        .agg(pl.len().alias("order_count"))
        .sort("o_orderpriority")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
