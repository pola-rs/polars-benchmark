import polars as pl
from linetimer import linetimer

from polars_queries import datasets


@linetimer(name="Query 03", unit='s')
def q03():
    customer_ds = (datasets.get_customer_ds()
                   .filter(pl.col("c_mktsegment") == "HOUSEHOLD")
                   .select(["c_custkey", "c_mktsegment"])
                   )

    line_item_ds = (datasets.get_line_item_ds()
                    .select(["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"])
                    .filter(pl.col("l_shipdate") > pl.datatypes.datetime(1995, 3, 4)))

    orders_ds = (datasets.get_orders_ds()
                 .select(["o_orderkey", "o_orderdate", "o_shippriority", "o_custkey"])
                 .filter(pl.col("o_orderdate") < pl.datatypes.datetime(1995, 3, 4)))

    df = (customer_ds
          .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
          .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
          .with_column((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue"))
          .groupby(["o_orderkey", "o_orderdate", "o_shippriority"])
          .agg([pl.sum("revenue")])
          .select([pl.col("o_orderkey").alias("l_orderkey"), "revenue", "o_orderdate", "o_shippriority"])
          # .sort([pl.col("revenue").sort_by(), "o_orderdate"])
          .limit(10)
          )

    print(df.collect())


if __name__ == '__main__':
    q03()
