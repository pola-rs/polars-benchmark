from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 3


def q() -> None:
    customer = utils.get_customer_ds()
    lineitem = utils.get_line_item_ds()
    orders = utils.get_orders_ds()

    var1 = "BUILDING"
    var2 = date(1995, 3, 15)

    q_final = (
        customer.filter(pl.col("c_mktsegment") == var1)
        .join(orders, left_on="c_custkey", right_on="o_custkey")
        .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("o_orderdate") < var2)
        .filter(pl.col("l_shipdate") > var2)
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .group_by("o_orderkey", "o_orderdate", "o_shippriority")
        .agg(pl.sum("revenue"))
        .select(
            pl.col("o_orderkey").alias("l_orderkey"),
            "revenue",
            "o_orderdate",
            "o_shippriority",
        )
        .sort(by=["revenue", "o_orderdate"], descending=[True, False])
        .head(10)
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
