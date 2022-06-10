import polars as pl
from linetimer import CodeTimer
from linetimer import linetimer

from polars_queries import utils

Q_NUM = 3


@linetimer(name=f"Overall execution of Query {Q_NUM}", unit='s')
def q3():
    var1 = var2 = "1995-03-15"
    var3 = "BUILDING"

    customer_ds = utils.get_customer_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()

    with CodeTimer(name=f"Get result of Query {Q_NUM}", unit='s'):
        result_df = (customer_ds
                     .filter(pl.col("c_mktsegment") == var3)
                     .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
                     .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
                     .filter(pl.col("o_orderdate") < var2)
                     .filter(pl.col("l_shipdate") > var1)
                     .with_column((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue"))
                     .groupby(["o_orderkey", "o_orderdate", "o_shippriority"])
                     .agg([pl.sum("revenue").round(2)])
                     .select([pl.col("o_orderkey").alias("l_orderkey"), "revenue", "o_orderdate", "o_shippriority"])
                     .sort(by=["revenue", "o_orderdate"], reverse=[True, False])
                     .limit(10)
                     ).collect()

        print(result_df)

    utils.test_results(Q_NUM, result_df)


if __name__ == '__main__':
    q3()
