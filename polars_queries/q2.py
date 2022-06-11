import polars as pl
from linetimer import CodeTimer
from linetimer import linetimer

from polars_queries import polars_tpch_utils

Q_NUM = 2


@linetimer(name=f"Overall execution of Query {Q_NUM}", unit='s')
def q():
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    region_ds = polars_tpch_utils.get_region_ds()
    nation_ds = polars_tpch_utils.get_nation_ds()
    supplier_ds = polars_tpch_utils.get_supplier_ds()
    part_ds = polars_tpch_utils.get_part_ds()
    part_supp_ds = polars_tpch_utils.get_part_supp_ds()

    with CodeTimer(name=f"Get result of Query {Q_NUM}", unit='s'):
        result_df = (part_ds
                     .join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
                     .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
                     .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
                     .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
                     .filter(pl.col("p_size") == var1)
                     .filter(pl.col("p_type").str.contains(f"{var2}$"))
                     .filter(pl.col("r_name") == var3))

        final_cols = ["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"]

        result_df2 = (result_df
                      .groupby("p_partkey")
                      .agg(pl.min("ps_supplycost").alias("ps_supplycost"))
                      .join(result_df, left_on=["p_partkey", "ps_supplycost"], right_on=["p_partkey", "ps_supplycost"])
                      .select(final_cols)
                      .sort(by=["s_acctbal", "n_name", "s_name", "p_partkey"], reverse=[True, False, False, False])
                      .limit(100)
                      .with_column(pl.col(pl.datatypes.Utf8).str.strip().keep_name())
                      ).collect()

        print(result_df2)
        polars_tpch_utils.test_results(Q_NUM, result_df2)


if __name__ == '__main__':
    q()
