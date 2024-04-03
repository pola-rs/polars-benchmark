from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from queries.dask import utils

if TYPE_CHECKING:
    import pandas as pd

Q_NUM = 3


def q() -> None:
    var1 = var2 = date(1995, 3, 15)
    var3 = "BUILDING"

    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds()
    line_item_ds()
    orders_ds()

    def query() -> pd.DataFrame:
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        customer_ds = customer_ds()
        line_item_ds = line_item_ds()
        orders_ds = orders_ds()

        lineitem_filtered = line_item_ds.loc[
            :, ["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"]
        ]
        orders_filtered = orders_ds.loc[
            :, ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"]
        ]
        customer_filtered = customer_ds.loc[:, ["c_mktsegment", "c_custkey"]]
        lsel = lineitem_filtered.l_shipdate > var1
        osel = orders_filtered.o_orderdate < var2
        csel = customer_filtered.c_mktsegment == var3
        flineitem = lineitem_filtered[lsel]
        forders = orders_filtered[osel]
        fcustomer = customer_filtered[csel]
        jn1 = fcustomer.merge(forders, left_on="c_custkey", right_on="o_custkey")
        jn2 = jn1.merge(flineitem, left_on="o_orderkey", right_on="l_orderkey")
        jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)

        # We have to deviate from pandas here because `groupby(as_index=False)` is not
        # implemented yet by Dask.
        # https://github.com/dask/dask/issues/5834
        total = (
            jn2.groupby(["l_orderkey", "o_orderdate", "o_shippriority"])["revenue"]
            .sum()
            .compute()
            .reset_index()
            .sort_values(["revenue"], ascending=False)
        )
        result_df = total[:10].loc[
            :, ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]
        ]
        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
