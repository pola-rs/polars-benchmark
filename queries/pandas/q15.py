from __future__ import annotations

import pandas as pd

from queries.pandas import utils

Q_NUM = 15


def q() -> None:
    lineitem_ds_fn = utils.get_line_item_ds
    supplier_ds_fn = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    lineitem_ds_fn()
    supplier_ds_fn()

    def query() -> pd.DataFrame:
        lineitem_ds = lineitem_ds_fn()
        supplier_ds = supplier_ds_fn()

        var1 = pd.Timestamp("1996-01-01")
        var2 = pd.Timestamp("1996-04-01")

        filtered_lineitem = lineitem_ds[
            (lineitem_ds["l_shipdate"] >= var1) & (lineitem_ds["l_shipdate"] < var2)
        ]

        filtered_lineitem["revenue"] = filtered_lineitem["l_extendedprice"] * (
            1 - filtered_lineitem["l_discount"]
        )

        revenue = filtered_lineitem.groupby("l_suppkey", as_index=False).agg(
            total_revenue=pd.NamedAgg(column="revenue", aggfunc="sum")
        )
        revenue = revenue.rename(columns={"l_suppkey": "supplier_no"})

        max_revenue = revenue["total_revenue"].max()

        jn = supplier_ds.merge(revenue, left_on="s_suppkey", right_on="supplier_no")
        jn = jn[jn["total_revenue"] == max_revenue]

        result = jn.loc[
            :, ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
        ]

        result_df = result.sort_values("s_suppkey")

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
