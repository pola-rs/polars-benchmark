from datetime import datetime

import polars as pl

from polars_queries import polars_tpch_utils

Q_NUM = 4


def q():
    var1 = datetime(1993, 7, 1)
    var2 = datetime(1993, 10, 1)

    line_item_ds = polars_tpch_utils.get_line_item_ds()
    orders_ds = polars_tpch_utils.get_orders_ds()

    q_final = (
        line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("o_orderdate") >= var1)
        .filter(pl.col("o_orderdate") < var2)
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .unique(subset=["o_orderpriority", "l_orderkey"])
        .groupby("o_orderpriority")
        .agg(pl.count().alias("order_count"))
        .sort(by="o_orderpriority")
        .with_column(pl.col("order_count").cast(pl.datatypes.Int64))
    )

    polars_tpch_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
