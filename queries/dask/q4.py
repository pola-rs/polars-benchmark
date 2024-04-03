from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from queries.dask import utils

if TYPE_CHECKING:
    import pandas as pd

Q_NUM = 4


def q() -> None:
    date1 = date(1993, 10, 1)
    date2 = date(1993, 7, 1)

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # First call one time to cache in case we don't include the IO times
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

        # `isin(Series)` is not yet implemented by Dask.
        # https://github.com/dask/dask/issues/4227
        forders = forders[["o_orderkey", "o_orderpriority"]]
        jn = forders.merge(
            flineitem, left_on="o_orderkey", right_on="l_orderkey"
        ).drop_duplicates(subset=["o_orderkey"])[["o_orderpriority", "o_orderkey"]]

        # `groupby(as_index=False)` is not yet implemented by Dask:
        # https://github.com/dask/dask/issues/5834
        result_df = (
            jn.groupby("o_orderpriority")["o_orderkey"]
            .count()
            .reset_index()
            .sort_values(["o_orderpriority"])
            .rename(columns={"o_orderkey": "order_count"})
        )
        return result_df.compute()  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
