from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 15


def q() -> None:
    lineitem = utils.get_line_item_ds()
    supplier = utils.get_supplier_ds()

    var1 = date(1996, 1, 1)
    var2 = date(1996, 4, 1)

    revenue = (
        lineitem.filter(pl.col("l_shipdate").is_between(var1, var2, closed="left"))
        .group_by("l_suppkey")
        .agg(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
            .sum()
            .alias("total_revenue")
        )
        .select(pl.col("l_suppkey").alias("supplier_no"), pl.col("total_revenue"))
    )

    q_final = (
        supplier.join(revenue, left_on="s_suppkey", right_on="supplier_no")
        .filter(pl.col("total_revenue") == pl.col("total_revenue").max())
        .with_columns(pl.col("total_revenue").round(2))
        .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
        .sort("s_suppkey")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
