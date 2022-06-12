from datetime import datetime
from typing import Union

import polars as pl
from pandas.core.frame import DataFrame as PandasDF
from polars.internals.frame import LazyFrame as PolarsLazyDF

from polars_tpch_benchmarks import BenchmarkQuery, datasets


class Q1(BenchmarkQuery):
    def get_query_result(self) -> Union[PolarsLazyDF, PandasDF]:
        line_item_ds: PolarsLazyDF = datasets.get_line_item_ds(azz=PolarsLazyDF)

        return (
            line_item_ds.filter(pl.col("l_shipdate") <= datetime(1998, 12, 1))
            .groupby(["l_returnflag", "l_linestatus"])
            .agg(
                [
                    pl.sum("l_quantity").alias("sum_qty"),
                    pl.sum("l_extendedprice").alias("sum_base_price"),
                    (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                    .sum()
                    .alias("sum_disc_price"),
                    (
                            pl.col("l_extendedprice")
                            * (1.0 - pl.col("l_discount") * (1.0 - pl.col("l_tax")))
                    )
                    .sum()
                    .alias("sum_charge"),
                    pl.mean("l_quantity").alias("avg_qty"),
                    pl.mean("l_extendedprice").alias("avg_price"),
                    pl.mean("l_discount").alias("avg_disc"),
                    pl.count().alias("count_order"),
                ],
            )
            .sort(["l_returnflag", "l_linestatus"])
        )


if __name__ == "__main__":
    Q1(1).execute_polars_query()
