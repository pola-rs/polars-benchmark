from __future__ import annotations

from typing import TYPE_CHECKING

from queries.modin import utils

if TYPE_CHECKING:
    import pandas as pd

Q_NUM = 2


def q() -> None:
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    region_ds = utils.get_region_ds
    nation_ds = utils.get_nation_ds
    supplier_ds = utils.get_supplier_ds
    part_ds = utils.get_part_ds
    part_supp_ds = utils.get_part_supp_ds

    # first call one time to cache in case we don't include the IO times
    region_ds()
    nation_ds()
    supplier_ds()
    part_ds()
    part_supp_ds()

    def query() -> pd.DataFrame:
        nonlocal region_ds
        nonlocal nation_ds
        nonlocal supplier_ds
        nonlocal part_ds
        nonlocal part_supp_ds
        region_ds = region_ds()
        nation_ds = nation_ds()
        supplier_ds = supplier_ds()
        part_ds = part_ds()
        part_supp_ds = part_supp_ds()

        nation_filtered = nation_ds.loc[:, ["n_nationkey", "n_name", "n_regionkey"]]
        region_filtered = region_ds[(region_ds["r_name"] == var3)]
        region_filtered = region_filtered.loc[:, ["r_regionkey"]]
        r_n_merged = nation_filtered.merge(
            region_filtered, left_on="n_regionkey", right_on="r_regionkey", how="inner"
        )
        r_n_merged = r_n_merged.loc[:, ["n_nationkey", "n_name"]]
        supplier_filtered = supplier_ds.loc[
            :,
            [
                "s_suppkey",
                "s_name",
                "s_address",
                "s_nationkey",
                "s_phone",
                "s_acctbal",
                "s_comment",
            ],
        ]
        s_r_n_merged = r_n_merged.merge(
            supplier_filtered,
            left_on="n_nationkey",
            right_on="s_nationkey",
            how="inner",
        )
        s_r_n_merged = s_r_n_merged.loc[
            :,
            [
                "n_name",
                "s_suppkey",
                "s_name",
                "s_address",
                "s_phone",
                "s_acctbal",
                "s_comment",
            ],
        ]
        partsupp_filtered = part_supp_ds.loc[
            :, ["ps_partkey", "ps_suppkey", "ps_supplycost"]
        ]
        ps_s_r_n_merged = s_r_n_merged.merge(
            partsupp_filtered, left_on="s_suppkey", right_on="ps_suppkey", how="inner"
        )
        ps_s_r_n_merged = ps_s_r_n_merged.loc[
            :,
            [
                "n_name",
                "s_name",
                "s_address",
                "s_phone",
                "s_acctbal",
                "s_comment",
                "ps_partkey",
                "ps_supplycost",
            ],
        ]
        part_filtered = part_ds.loc[:, ["p_partkey", "p_mfgr", "p_size", "p_type"]]
        part_filtered = part_filtered[
            (part_filtered["p_size"] == var1)
            & (part_filtered["p_type"].str.endswith(var2))
        ]
        part_filtered = part_filtered.loc[:, ["p_partkey", "p_mfgr"]]
        merged_df = part_filtered.merge(
            ps_s_r_n_merged, left_on="p_partkey", right_on="ps_partkey", how="inner"
        )
        merged_df = merged_df.loc[
            :,
            [
                "n_name",
                "s_name",
                "s_address",
                "s_phone",
                "s_acctbal",
                "s_comment",
                "ps_supplycost",
                "p_partkey",
                "p_mfgr",
            ],
        ]
        min_values = merged_df.groupby("p_partkey", as_index=False)[
            "ps_supplycost"
        ].min()
        min_values.columns = ["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]
        merged_df = merged_df.merge(
            min_values,
            left_on=["p_partkey", "ps_supplycost"],
            right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"],
            how="inner",
        )
        result_df = merged_df.loc[
            :,
            [
                "s_acctbal",
                "s_name",
                "n_name",
                "p_partkey",
                "p_mfgr",
                "s_address",
                "s_phone",
                "s_comment",
            ],
        ]
        result_df = result_df.sort_values(
            by=[
                "s_acctbal",
                "n_name",
                "s_name",
                "p_partkey",
            ],
            ascending=[
                False,
                True,
                True,
                True,
            ],
        )[:100]

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
