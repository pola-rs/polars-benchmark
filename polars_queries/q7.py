from datetime import datetime

import polars as pl

from polars_queries import polars_tpch_utils

Q_NUM = 7


def q():
    nation_ds = polars_tpch_utils.get_nation_ds()
    customer_ds = polars_tpch_utils.get_customer_ds()
    line_item_ds = polars_tpch_utils.get_line_item_ds()
    orders_ds = polars_tpch_utils.get_orders_ds()
    supplier_ds = polars_tpch_utils.get_supplier_ds()

    n1 = nation_ds.filter(pl.col("n_name") == "FRANCE")
    n2 = nation_ds.filter(pl.col("n_name") == "GERMANY")

    df1 = (customer_ds.join(n1, left_on="c_nationkey", right_on="n_nationkey")
           .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
           .rename({"n_name": "cust_nation"})
           .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
           .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
           .join(n2, left_on="s_nationkey", right_on="n_nationkey")
           .rename({"n_name": "supp_nation"})
           )

    df2 = (customer_ds.join(n2, left_on="c_nationkey", right_on="n_nationkey")
           .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
           .rename({"n_name": "cust_nation"})
           .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
           .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
           .join(n1, left_on="s_nationkey", right_on="n_nationkey")
           .rename({"n_name": "supp_nation"})
           )

    q_final = (
        pl.concat([df1, df2])
        .filter(pl.col("l_shipdate") >= datetime(1995, 1, 1))
        .filter(pl.col("l_shipdate") <= datetime(1996, 12, 31))
        .with_column((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume"))
        .with_column(pl.col("l_shipdate").dt.year().alias("l_year"))
        .groupby(["supp_nation", "cust_nation", "l_year"])
        .agg([pl.sum("volume").alias("revenue")])
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )

    polars_tpch_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
