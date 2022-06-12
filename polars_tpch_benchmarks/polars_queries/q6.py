from datetime import datetime
from typing import Union

import polars as pl
from pandas.core.frame import DataFrame as PandasDF
from polars.internals.frame import LazyFrame as PolarsLazyDF

from polars_tpch_benchmarks import BenchmarkQuery, datasets


class Q6(BenchmarkQuery):
    def get_query_result(self) -> Union[PolarsLazyDF, PandasDF]:
        var1 = datetime(1994, 1, 1)
        var2 = datetime(1995, 1, 1)
        var3 = 24

        line_item_ds: PolarsLazyDF = datasets.get_line_item_ds(azz=PolarsLazyDF)

        return (
            line_item_ds.filter(pl.col("l_shipdate") >= var1)
            .filter(pl.col("l_shipdate") < var2)
            .filter((pl.col("l_discount") >= 0.05) & (pl.col("l_discount") <= 0.07))
            .filter(pl.col("l_quantity") < var3)
            .with_column(
                (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
            )
            .select(pl.sum("revenue").alias("revenue"))
        )


if __name__ == "__main__":
    Q6(6).execute_polars_query()
