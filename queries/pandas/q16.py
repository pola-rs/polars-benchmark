from __future__ import annotations

import pandas as pd

from queries.pandas import utils

Q_NUM = 16


def q() -> None:
    part_ds_fn = utils.get_part_ds
    part_supp_ds_fn = utils.get_part_supp_ds
    supplier_ds_fn = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    part_ds_fn()
    part_supp_ds_fn()
    supplier_ds_fn()

    def query() -> pd.DataFrame:
        part_ds = part_ds_fn()
        part_supp_ds = part_supp_ds_fn()
        supplier_ds = supplier_ds_fn()

        var1 = "Brand#45"

        # Filter suppliers with complaints
        filtered_supplier = supplier_ds[
            supplier_ds["s_comment"].str.contains(
                ".*Customer.*Complaints.*", regex=True, na=False
            )
        ][["s_suppkey"]]

        jn = part_ds.merge(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")

        jn = jn[jn["p_brand"] != var1]
        jn = jn[~jn["p_type"].str.startswith("MEDIUM POLISHED")]
        jn = jn[jn["p_size"].isin([49, 14, 23, 45, 19, 3, 36, 9])]

        # Left join to exclude suppliers with complaints
        jn2 = jn.merge(
            filtered_supplier,
            left_on="ps_suppkey",
            right_on="s_suppkey",
            how="left",
        )
        jn2 = jn2[jn2["s_suppkey"].isna()]

        gb = jn2.groupby(["p_brand", "p_type", "p_size"], as_index=False)
        agg = gb.agg(supplier_cnt=pd.NamedAgg(column="ps_suppkey", aggfunc="nunique"))

        result_df = agg.sort_values(
            by=["supplier_cnt", "p_brand", "p_type", "p_size"],
            ascending=[False, True, True, True],
        )

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
