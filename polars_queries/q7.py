from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 7


def q():
    nation_ds = utils.get_nation_ds()
    customer_ds = utils.get_customer_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    supplier_ds = utils.get_supplier_ds()

    n1 = nation_ds.filter(pl.col("n_name") == "FRANCE")
    n2 = nation_ds.filter(pl.col("n_name") == "GERMANY")

    var_1 = datetime(1995, 1, 1)
    var_2 = datetime(1996, 12, 31)

    df1 = (
        customer_ds.join(n1, left_on="c_nationkey", right_on="n_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .rename({"n_name": "cust_nation"})
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(n2, left_on="s_nationkey", right_on="n_nationkey")
        .rename({"n_name": "supp_nation"})
    )

    df2 = (
        customer_ds.join(n2, left_on="c_nationkey", right_on="n_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .rename({"n_name": "cust_nation"})
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(n1, left_on="s_nationkey", right_on="n_nationkey")
        .rename({"n_name": "supp_nation"})
    )

    q_final = (
        pl.concat([df1, df2])
        .filter(pl.col("l_shipdate").is_between(var_1, var_2))
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume")
        )
        .with_columns(pl.col("l_shipdate").dt.year().alias("l_year"))
        .group_by(["supp_nation", "cust_nation", "l_year"])
        .agg([pl.sum("volume").alias("revenue")])
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
