from vaex_queries import utils

Q_NUM = 2


def q():
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

    def query():
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

        nation_filtered = nation_ds[["n_nationkey", "n_name", "n_regionkey"]]
        region_filtered = region_ds[(region_ds["r_name"] == var3)]
        region_filtered = region_filtered[["r_regionkey"]]
        r_n_merged = nation_filtered.join(
            region_filtered, left_on="n_regionkey", right_on="r_regionkey", how="inner"
        )
        r_n_merged = r_n_merged[["n_nationkey", "n_name"]]

        supplier_filtered = supplier_ds[
            [
                "s_suppkey",
                "s_name",
                "s_address",
                "s_nationkey",
                "s_phone",
                "s_acctbal",
                "s_comment",
            ]
        ]
        s_r_n_merged = r_n_merged.join(
            supplier_filtered,
            left_on="n_nationkey",
            right_on="s_nationkey",
            how="inner",
            allow_duplication=True,
        )
        s_r_n_merged = s_r_n_merged[
            [
                "n_name",
                "s_suppkey",
                "s_name",
                "s_address",
                "s_phone",
                "s_acctbal",
                "s_comment",
            ]
        ]
        partsupp_filtered = part_supp_ds[["ps_partkey", "ps_suppkey", "ps_supplycost"]]
        ps_s_r_n_merged = s_r_n_merged.join(
            partsupp_filtered,
            left_on="s_suppkey",
            right_on="ps_suppkey",
            how="inner",
            allow_duplication=True,
        )
        ps_s_r_n_merged = ps_s_r_n_merged[
            [
                "n_name",
                "s_name",
                "s_address",
                "s_phone",
                "s_acctbal",
                "s_comment",
                "ps_partkey",
                "ps_supplycost",
            ]
        ]
        part_filtered = part_ds[["p_partkey", "p_mfgr", "p_size", "p_type"]]
        part_filtered = part_filtered[
            (part_filtered["p_size"] == var1)
            & (part_filtered["p_type"].str.endswith(var2))
        ]
        part_filtered = part_filtered[["p_partkey", "p_mfgr"]]
        # see: https://github.com/vaexio/vaex/issues/1319
        part_filtered = part_filtered.sort("p_partkey")

        merged_df = part_filtered.join(
            ps_s_r_n_merged,
            left_on="p_partkey",
            right_on="ps_partkey",
            how="inner",
            allow_duplication=True,
        )
        merged_df = merged_df[
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
            ]
        ]
        min_values = ps_s_r_n_merged.groupby("ps_partkey").agg({"ps_supplycost": "min"})
        merged_df = merged_df.join(
            min_values,
            left_on=["p_partkey", "ps_supplycost"],
            right_on=["p_partkey", "ps_supplycost"],
            how="inner",
            allow_duplication=True,
        )
        result_df = merged_df[
            [
                "s_acctbal",
                "s_name",
                "n_name",
                "p_partkey",
                "p_mfgr",
                "s_address",
                "s_phone",
                "s_comment",
            ]
        ].sort(
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
        )[
            :100
        ]

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
