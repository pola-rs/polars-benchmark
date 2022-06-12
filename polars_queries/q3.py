from datetime import datetime

import polars as pl

from polars_queries import polars_tpch_utils

Q_NUM = 3


def q():
    var1 = var2 = datetime(1995, 3, 15)
    var3 = "BUILDING"

    customer_ds = polars_tpch_utils.get_customer_ds()
    line_item_ds = polars_tpch_utils.get_line_item_ds()
    orders_ds = polars_tpch_utils.get_orders_ds()

    q_final = (
        customer_ds.filter(pl.col("c_mktsegment") == var3)
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("o_orderdate") < var2)
        .filter(pl.col("l_shipdate") > var1)
        .with_column(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .groupby(["o_orderkey", "o_orderdate", "o_shippriority"])
        .agg([pl.sum("revenue")])
        .select(
            [
                pl.col("o_orderkey").alias("l_orderkey"),
                "revenue",
                "o_orderdate",
                "o_shippriority",
            ]
        )
        .sort(by=["revenue", "o_orderdate"], reverse=[True, False])
        .limit(10)
    )

    polars_tpch_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
