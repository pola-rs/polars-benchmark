from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 4


def q(
    customer: None | pl.LazyFrame,
    lineitem: None | pl.LazyFrame,
    nation: None | pl.LazyFrame,
    orders: None | pl.LazyFrame,
    partsupp: None | pl.LazyFrame,
    supplier: None | pl.LazyFrame,
    region: None | pl.LazyFrame,
    part: None | pl.LazyFrame,
    **kwargs

) -> pl.LazyFrame:
    if lineitem is None:
        lineitem = utils.get_line_item_ds()
        orders = utils.get_orders_ds()

    var1 = date(1993, 7, 1)
    var2 = date(1993, 10, 1)

    return (
        # SQL exists translates to semi join in Polars API
        orders.join(
            (lineitem.filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))),
            left_on="o_orderkey",
            right_on="l_orderkey",
            how="semi",
        )
        .filter(pl.col("o_orderdate").is_between(var1, var2, closed="left"))
        .group_by("o_orderpriority")
        .agg(pl.len().alias("order_count"))
        .sort("o_orderpriority")
    )


if __name__ == "__main__":
    utils.run_query(Q_NUM, q())
