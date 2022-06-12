from datetime import datetime
from typing import Union

import polars as pl
from pandas.core.frame import DataFrame as PandasDF
from polars.internals.frame import LazyFrame as PolarsLazyDF

from polars_tpch_benchmarks import BenchmarkQuery, datasets


class Q3(BenchmarkQuery):
    def get_query_result(self) -> Union[PolarsLazyDF, PandasDF]:
        var1 = var2 = datetime(1995, 3, 15)
        var3 = "BUILDING"

        customer_ds = datasets.get_customer_ds(azz=PolarsLazyDF)
        line_item_ds = datasets.get_line_item_ds(azz=PolarsLazyDF)
        orders_ds = datasets.get_orders_ds(azz=PolarsLazyDF)

        return (
            customer_ds.filter(pl.col("c_mktsegment") == var3)
            .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .filter(pl.col("o_orderdate") < var2)
            .filter(pl.col("l_shipdate") > var1)
            .with_column(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias(
                    "revenue"
                )
            )
            .groupby(["o_orderkey", "o_orderdate", "o_shippriority"])
            .agg([pl.sum("revenue")])
            .select(
                [
                    pl.col("o_orderkey").alias("l_orderkey"),
                    "revenue",
                    "o_orderdate",
                    "o_shippriority",
                ]
            )
            .sort(by=["revenue", "o_orderdate"], reverse=[True, False])
            .limit(10)
        )


if __name__ == "__main__":
    Q3(3).execute_polars_query()
