from __future__ import annotations

from datetime import date

import pandas as pd

from queries.dask import utils

Q_NUM = 1


def q() -> None:
    VAR1 = date(1998, 9, 2)

    lineitem = utils.get_line_item_ds
    # first call one time to cache in case we don't include the IO times
    lineitem()

    def query() -> pd.DataFrame:
        nonlocal lineitem
        lineitem = lineitem()

        sel = lineitem.l_shipdate <= VAR1
        lineitem_filtered = lineitem[sel]

        # This is lenient towards pandas as normally an optimizer should decide
        # that this could be computed before the groupby aggregation.
        # Other implementations don't enjoy this benefit.
        lineitem_filtered["disc_price"] = lineitem_filtered.l_extendedprice * (
            1 - lineitem_filtered.l_discount
        )
        lineitem_filtered["charge"] = (
            lineitem_filtered.l_extendedprice
            * (1 - lineitem_filtered.l_discount)
            * (1 + lineitem_filtered.l_tax)
        )

        # `groupby(as_index=False)` is not yet implemented by Dask:
        # https://github.com/dask/dask/issues/5834
        gb = lineitem_filtered.groupby(["l_returnflag", "l_linestatus"])

        total = gb.agg(
            sum_qty=pd.NamedAgg(column="l_quantity", aggfunc="sum"),
            sum_base_price=pd.NamedAgg(column="l_extendedprice", aggfunc="sum"),
            sum_disc_price=pd.NamedAgg(column="disc_price", aggfunc="sum"),
            sum_charge=pd.NamedAgg(column="charge", aggfunc="sum"),
            avg_qty=pd.NamedAgg(column="l_quantity", aggfunc="mean"),
            avg_price=pd.NamedAgg(column="l_extendedprice", aggfunc="mean"),
            avg_disc=pd.NamedAgg(column="l_discount", aggfunc="mean"),
            count_order=pd.NamedAgg(column="l_orderkey", aggfunc="size"),
        )

        result_df = (
            total.compute().reset_index().sort_values(["l_returnflag", "l_linestatus"])
        )

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
