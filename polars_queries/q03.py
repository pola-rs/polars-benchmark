import polars as pl
from linetimer import linetimer

from polars_queries import datasets


@linetimer(name="Query 03", unit='s')
def q03():
    var1 = var2 = pl.datatypes.datetime(1995, 3, 15)
    var3 = "BUILDING"

    customer_ds = datasets.get_customer_ds()
    line_item_ds = datasets.get_line_item_ds()
    orders_ds = datasets.get_orders_ds()

    df = (customer_ds
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
          )

    print(df.collect())


if __name__ == '__main__':
    q03()
