from datetime import datetime
from typing import Union

import polars as pl
from pandas.core.frame import DataFrame as PandasDF
from polars.internals.frame import LazyFrame as PolarsLazyDF

from polars_tpch_benchmarks import BenchmarkQuery, datasets


class Q5(BenchmarkQuery):
    def get_query_result(self) -> Union[PolarsLazyDF, PandasDF]:
        var1 = "ASIA"
        var2 = datetime(1994, 1, 1)
        var3 = datetime(1995, 1, 1)

        customer_ds = datasets.get_customer_ds(azz=PolarsLazyDF)
        line_item_ds = datasets.get_line_item_ds(azz=PolarsLazyDF)
        orders_ds = datasets.get_orders_ds(azz=PolarsLazyDF)
        region_ds = datasets.get_region_ds(azz=PolarsLazyDF)
        nation_ds = datasets.get_nation_ds(azz=PolarsLazyDF)
        supplier_ds = datasets.get_supplier_ds(azz=PolarsLazyDF)

        return (
            region_ds.join(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
            .join(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
            .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .join(
                supplier_ds,
                left_on=["l_suppkey", "n_nationkey"],
                right_on=["s_suppkey", "s_nationkey"],
            )
            .filter(pl.col("r_name") == var1)
            .filter(pl.col("o_orderdate") >= var2)
            .filter(pl.col("o_orderdate") < var3)
            .with_column(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias(
                    "revenue"
                )
            )
            .groupby("n_name")
            .agg([pl.sum("revenue")])
            .sort(by="revenue", reverse=True)
        )


if __name__ == "__main__":
    Q5(5).execute_polars_query()
