import polars as pl
from linetimer import CodeTimer
from linetimer import linetimer

from polars_queries import polars_tpch_utils

Q_NUM = 4


@linetimer(name=f"Overall execution of Query {Q_NUM}", unit='s')
def q():
    var1 = "1993-07-01"
    var2 = "1993-10-01"

    line_item_ds = polars_tpch_utils.get_line_item_ds()
    orders_ds = polars_tpch_utils.get_orders_ds()

    with CodeTimer(name=f"Get result of Query {Q_NUM}", unit='s'):
        result_df = (line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
                     .filter(pl.col("o_orderdate") >= var1)
                     .filter(pl.col("o_orderdate") < var2)
                     .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
                     .unique(subset=["o_orderpriority", "l_orderkey"])
                     .groupby("o_orderpriority")
                     .agg(pl.count().alias("order_count"))
                     .sort(by="o_orderpriority")
                     .with_column(pl.col("order_count").cast(pl.datatypes.Int64))
                     ).collect()

        print(result_df)
    polars_tpch_utils.test_results(Q_NUM, result_df)


if __name__ == '__main__':
    q()
