from __future__ import annotations

import pandas as pd

from queries.pandas import utils
from settings import Settings

Q_NUM = 11

settings = Settings()


def q() -> None:
    nation_ds_fn = utils.get_nation_ds
    part_supp_ds_fn = utils.get_part_supp_ds
    supplier_ds_fn = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    nation_ds_fn()
    part_supp_ds_fn()
    supplier_ds_fn()

    def query() -> pd.DataFrame:
        nation_ds = nation_ds_fn()
        part_supp_ds = part_supp_ds_fn()
        supplier_ds = supplier_ds_fn()

        var1 = "GERMANY"
        var2 = 0.0001 / settings.scale_factor

        jn1 = part_supp_ds.merge(
            supplier_ds, left_on="ps_suppkey", right_on="s_suppkey"
        )
        jn2 = jn1.merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        jn2 = jn2[jn2["n_name"] == var1]

        jn2["value"] = jn2["ps_supplycost"] * jn2["ps_availqty"]

        threshold = jn2["value"].sum() * var2

        gb = jn2.groupby("ps_partkey", as_index=False)
        agg = gb.agg(value=pd.NamedAgg(column="value", aggfunc="sum"))

        result = agg[agg["value"] > threshold]
        result_df = result.sort_values("value", ascending=False)

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
