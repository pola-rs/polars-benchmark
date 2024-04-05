from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 3


def q() -> None:
    var_1 = "BUILDING"
    var_2 = date(1995, 3, 15)

    customer_ds = utils.get_customer_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()

    q_final = (
        customer_ds.filter(pl.col("c_mktsegment") == var_1)
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("o_orderdate") < var_2)
        .filter(pl.col("l_shipdate") > var_2)
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
        .limit(10)
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
