from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 4


def q() -> None:
    var_1 = date(1993, 7, 1)
    var_2 = date(1993, 10, 1)

    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()

    q_final = (
        line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("o_orderdate").is_between(var_1, var_2, closed="left"))
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .unique(subset=["o_orderpriority", "l_orderkey"])
        .group_by("o_orderpriority")
        .agg(pl.len().alias("order_count"))
        .sort("o_orderpriority")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
