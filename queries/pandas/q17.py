from __future__ import annotations

import pandas as pd

from queries.pandas import utils

Q_NUM = 17


def q() -> None:
    lineitem_ds_fn = utils.get_line_item_ds
    part_ds_fn = utils.get_part_ds

    # first call one time to cache in case we don't include the IO times
    lineitem_ds_fn()
    part_ds_fn()

    def query() -> pd.DataFrame:
        lineitem_ds = lineitem_ds_fn()
        part_ds = part_ds_fn()

        var1 = "Brand#23"
        var2 = "MED BOX"

        filtered_part = part_ds[
            (part_ds["p_brand"] == var1) & (part_ds["p_container"] == var2)
        ]

        jn = filtered_part.merge(lineitem_ds, left_on="p_partkey", right_on="l_partkey")

        # Calculate average quantity per partkey
        avg_qty = jn.groupby("p_partkey", as_index=False).agg(
            avg_quantity=pd.NamedAgg(column="l_quantity", aggfunc="mean")
        )
        avg_qty["avg_quantity"] = 0.2 * avg_qty["avg_quantity"]

        jn2 = jn.merge(avg_qty, on="p_partkey")
        jn2 = jn2[jn2["l_quantity"] < jn2["avg_quantity"]]

        avg_yearly = round(jn2["l_extendedprice"].sum() / 7.0, 2)

        result_df = pd.DataFrame({"avg_yearly": [avg_yearly]})

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
