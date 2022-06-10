import polars as pl
from linetimer import linetimer


# from polars_queries import datasets


@linetimer(name="Query 04", unit='s')
def q04():
    var1 = "1993-07-01"
    var2 = "1993-10-01"

    # line_item_ds = datasets.get_line_item_ds()
    # orders_ds = datasets.get_orders_ds()

    line_item_ds = pl.scan_parquet("/Users/chitralverma/IdeaProjects/tpch/tables_scale_1/lineitem.parquet")
    orders_ds = pl.scan_parquet("/Users/chitralverma/IdeaProjects/tpch/tables_scale_1/orders.parquet")

    df = (line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
          .filter(pl.col("o_orderdate") >= var1)
          .filter(pl.col("o_orderdate") < var2)
          .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
          .unique(subset=["o_orderpriority", "l_orderkey"])
          .groupby("o_orderpriority")
          .agg(pl.count().alias("order_count"))
          .sort(by="o_orderpriority")
          )

    print(df.collect())


if __name__ == '__main__':
    q04()
