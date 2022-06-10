from datetime import datetime

import polars as pl
from linetimer import CodeTimer
from linetimer import linetimer

from polars_queries import polars_tpch_utils

Q_NUM = 5


@linetimer(name=f"Overall execution of Query {Q_NUM}", unit='s')
def q():
    var1 = "ASIA"
    var2 = datetime(1994, 1, 1)
    var3 = datetime(1995, 1, 1)

    region_ds = polars_tpch_utils.get_region_ds()
    nation_ds = polars_tpch_utils.get_nation_ds()
    customer_ds = polars_tpch_utils.get_customer_ds()
    line_item_ds = polars_tpch_utils.get_line_item_ds()
    orders_ds = polars_tpch_utils.get_orders_ds()
    supplier_ds = polars_tpch_utils.get_supplier_ds()

    with CodeTimer(name=f"Get result of Query {Q_NUM}", unit='s'):
        result_df = (region_ds
                     .join(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
                     .join(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
                     .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
                     .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
                     .join(supplier_ds, left_on=["l_suppkey", "n_nationkey"], right_on=["s_suppkey", "s_nationkey"])
                     .filter(pl.col("r_name") == var1)
                     .filter(pl.col("o_orderdate") >= var2)
                     .filter(pl.col("o_orderdate") < var3)
                     .with_column((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue"))
                     .groupby("n_name")
                     .agg([pl.sum("revenue")])
                     .sort(by="revenue", reverse=True)
                     ).collect()
        #
        print(result_df)
        polars_tpch_utils.test_results(Q_NUM, result_df)


if __name__ == '__main__':
    q()
