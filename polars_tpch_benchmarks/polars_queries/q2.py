from typing import Union

import polars as pl
from pandas.core.frame import DataFrame as PandasDF
from polars.internals.frame import LazyFrame as PolarsLazyDF

from polars_tpch_benchmarks import BenchmarkQuery, datasets


class Q2(BenchmarkQuery):
    def get_query_result(self) -> Union[PolarsLazyDF, PandasDF]:
        var1 = 15
        var2 = "BRASS"
        var3 = "EUROPE"

        region_ds = datasets.get_region_ds(azz=PolarsLazyDF)
        nation_ds = datasets.get_nation_ds(azz=PolarsLazyDF)
        supplier_ds = datasets.get_supplier_ds(azz=PolarsLazyDF)
        part_ds = datasets.get_part_ds(azz=PolarsLazyDF)
        part_supp_ds = datasets.get_part_supp_ds(azz=PolarsLazyDF)

        result_q1 = (
            part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
            .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
            .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
            .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
            .filter(pl.col("p_size") == var1)
            .filter(pl.col("p_type").str.contains(f"{var2}$"))
            .filter(pl.col("r_name") == var3)
        ).cache()

        final_cols = [
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        ]

        return (
            result_q1.groupby("p_partkey")
            .agg(pl.min("ps_supplycost").alias("ps_supplycost"))
            .join(
                result_q1,
                left_on=["p_partkey", "ps_supplycost"],
                right_on=["p_partkey", "ps_supplycost"],
            )
            .select(final_cols)
            .sort(
                by=["s_acctbal", "n_name", "s_name", "p_partkey"],
                reverse=[True, False, False, False],
            )
            .limit(100)
            .with_column(pl.col(pl.datatypes.Utf8).str.strip().keep_name())
        )


if __name__ == "__main__":
    Q2(2).execute_polars_query()
