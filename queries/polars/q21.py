from typing import Any

import polars as pl

from queries.polars import utils

Q_NUM = 21


def q(
    lineitem: None | pl.LazyFrame = None,
    nation: None | pl.LazyFrame = None,
    orders: None | pl.LazyFrame = None,
    supplier: None | pl.LazyFrame = None,
    **kwargs: Any,
) -> pl.LazyFrame:
    if lineitem is None:
        lineitem = utils.get_line_item_ds()
        nation = utils.get_nation_ds()
        orders = utils.get_orders_ds()
        supplier = utils.get_supplier_ds()

    assert lineitem is not None
    assert nation is not None
    assert orders is not None
    assert supplier is not None

    var1 = "SAUDI ARABIA"

    is_late = pl.col("l_receiptdate") > pl.col("l_commitdate")

    l1 = (
        lineitem.filter(is_late)
        .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
        .join(
            nation.filter(pl.col("n_name") == var1),
            left_on="s_nationkey",
            right_on="n_nationkey",
        )
        .join(
            orders.filter(pl.col("o_orderstatus") == "F"),
            left_on="l_orderkey",
            right_on="o_orderkey",
            how="semi",
        )
    )

    per_order = (
        lineitem.join(l1, on="l_orderkey", how="semi")
        .group_by("l_orderkey")
        .agg(
            n_supp=pl.col("l_suppkey").n_unique(),
            n_late_supp=pl.col("l_suppkey").filter(is_late).n_unique(),
        )
    )

    return (
        l1.join(per_order, on="l_orderkey")
        .filter(pl.col("n_supp") > 1, pl.col("n_late_supp") == 1)
        .group_by("s_name")
        .agg(pl.len().alias("numwait"))
        .sort(by=["numwait", "s_name"], descending=[True, False])
        .head(100)
    )


if __name__ == "__main__":
    utils.run_query(Q_NUM, q())
