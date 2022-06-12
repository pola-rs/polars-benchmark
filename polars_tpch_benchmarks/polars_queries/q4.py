from datetime import datetime
from typing import Union

import polars as pl
from pandas.core.frame import DataFrame as PandasDF
from polars.internals.frame import LazyFrame as PolarsLazyDF

from polars_tpch_benchmarks import BenchmarkQuery, datasets


class Q4(BenchmarkQuery):
    def get_query_result(self) -> Union[PolarsLazyDF, PandasDF]:
        var1 = datetime(1993, 7, 1)
        var2 = datetime(1993, 10, 1)

        line_item_ds = datasets.get_line_item_ds(azz=PolarsLazyDF)
        orders_ds = datasets.get_orders_ds(azz=PolarsLazyDF)

        return (
            line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
            .filter(pl.col("o_orderdate") >= var1)
            .filter(pl.col("o_orderdate") < var2)
            .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
            .unique(subset=["o_orderpriority", "l_orderkey"])
            .groupby("o_orderpriority")
            .agg(pl.count().alias("order_count"))
            .sort(by="o_orderpriority")
            .with_column(pl.col("order_count").cast(pl.datatypes.Int64))
        )


if __name__ == "__main__":
    Q4(4).execute_polars_query()
