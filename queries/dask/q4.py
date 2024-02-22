from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from queries.dask import utils

if TYPE_CHECKING:
    import pandas as pd

Q_NUM = 4


def q() -> None:
    date1 = datetime(1993, 10, 1)
    date2 = datetime(1993, 7, 1)

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        nonlocal orders_ds
        line_item_ds = line_item_ds()
        orders_ds = orders_ds()

        lsel = line_item_ds.l_commitdate < line_item_ds.l_receiptdate
        osel = (orders_ds.o_orderdate < date1) & (orders_ds.o_orderdate >= date2)
        flineitem = line_item_ds[lsel]
        forders = orders_ds[osel]
        forders = forders[["o_orderkey", "o_orderpriority"]]

        # doesn't support isin
        # jn = forders[forders["o_orderkey"].compute().isin(flineitem["l_orderkey"])]

        jn = forders.merge(
            flineitem, left_on="o_orderkey", right_on="l_orderkey"
        ).drop_duplicates(subset=["o_orderkey"])[["o_orderpriority", "o_orderkey"]]
        result_df = (
            jn.groupby("o_orderpriority")["o_orderkey"]
            .count()
            .reset_index()
            .sort_values(["o_orderpriority"])
        )
        result_df = result_df.compute()
        return result_df.rename({"o_orderkey": "order_count"}, axis=1)  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
