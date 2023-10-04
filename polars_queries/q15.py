from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 15


def q():
    line_item_ds = utils.get_line_item_ds()
    supplier_ds = utils.get_supplier_ds()

    var_1 = datetime(1996, 1, 1)
    var_2 = datetime(1996, 4, 1)

    revenue_ds = (
        line_item_ds.filter(
            pl.col("l_shipdate").is_between(var_1, var_2, closed="left")
        )
        .group_by("l_suppkey")
        .agg(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
            .sum()
            .alias("total_revenue")
        )
        .select([pl.col("l_suppkey").alias("supplier_no"), pl.col("total_revenue")])
    )

    q_final = (
        supplier_ds.join(revenue_ds, left_on="s_suppkey", right_on="supplier_no")
        .filter(pl.col("total_revenue") == pl.col("total_revenue").max())
        .with_columns(pl.col("total_revenue").round(2))
        .select(["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"])
        .sort("s_suppkey")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
