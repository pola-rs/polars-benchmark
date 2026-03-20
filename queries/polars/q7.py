from datetime import date
from typing import Any

import polars as pl

from queries.polars import utils

Q_NUM = 7


def q(
    customer: None | pl.LazyFrame = None,
    lineitem: None | pl.LazyFrame = None,
    nation: None | pl.LazyFrame = None,
    orders: None | pl.LazyFrame = None,
    supplier: None | pl.LazyFrame = None,
    region: None | pl.LazyFrame = None,
    **kwargs: Any,
) -> pl.LazyFrame:
    if customer is None:
        customer = utils.get_customer_ds()
        lineitem = utils.get_line_item_ds()
        nation = utils.get_nation_ds()
        orders = utils.get_orders_ds()
        supplier = utils.get_supplier_ds()
    assert lineitem is not None
    assert nation is not None
    assert orders is not None
    assert supplier is not None

    var1 = "FRANCE"
    var2 = "GERMANY"
    var3 = date(1995, 1, 1)
    var4 = date(1996, 12, 31)

    nations = nation.filter(pl.col("n_name").is_in([var1, var2]))

    return (
        customer.join(nations, left_on="c_nationkey", right_on="n_nationkey")
        .rename({"n_name": "cust_nation"})
        .join(orders, left_on="c_custkey", right_on="o_custkey")
        .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
        .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
        .join(nations, left_on="s_nationkey", right_on="n_nationkey")
        .rename({"n_name": "supp_nation"})
        .filter(
            ((pl.col("cust_nation") == var1) & (pl.col("supp_nation") == var2))
            | ((pl.col("cust_nation") == var2) & (pl.col("supp_nation") == var1))
        )
        .filter(pl.col("l_shipdate").is_between(var3, var4))
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume"),
            pl.col("l_shipdate").dt.year().alias("l_year"),
        )
        .group_by("supp_nation", "cust_nation", "l_year")
        .agg(pl.sum("volume").alias("revenue"))
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )


if __name__ == "__main__":
    utils.run_query(Q_NUM, q())
