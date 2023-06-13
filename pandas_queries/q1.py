from datetime import datetime

import pyarrow.compute as pc

from pandas_queries import utils

Q_NUM = 1


def q():
    VAR1 = datetime(1998, 9, 2)
    columns = [
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_orderkey",
    ]
    kwargs_lineitem = {"filters": pc.field("l_shipdate") <= VAR1}

    lineitem = utils.get_line_item_ds
    # first call one time to cache in case we don't include the IO times
    lineitem(columns=columns, kwargs=kwargs_lineitem)

    def query():
        nonlocal lineitem
        lineitem = lineitem(columns=columns, kwargs=kwargs_lineitem)
        lineitem_filtered = lineitem
        lineitem_filtered["avg_qty"] = lineitem_filtered.l_quantity
        lineitem_filtered["avg_price"] = lineitem_filtered.l_extendedprice
        lineitem_filtered["sum_disc_price"] = lineitem_filtered.l_extendedprice * (
            1 - lineitem_filtered.l_discount
        )

        lineitem_filtered["sum_charge"] = (
            lineitem_filtered.l_extendedprice
            * (1 - lineitem_filtered.l_discount)
            * (1 + lineitem_filtered.l_tax)
        )

        lineitem_filtered = lineitem_filtered.rename(
            columns={
                "l_quantity": "sum_qty",
                "l_extendedprice": "sum_base_price",
                "l_discount": "avg_disc",
                "l_orderkey": "count_order",
            }
        )

        gb = lineitem_filtered.groupby(["l_returnflag", "l_linestatus"])

        total = gb.agg(
            {
                "sum_qty": "sum",
                "sum_base_price": "sum",
                "sum_disc_price": "sum",
                "sum_charge": "sum",
                "avg_qty": "mean",
                "avg_price": "mean",
                "avg_disc": "mean",
                "count_order": "count",
            },
        )

        result_df = total.reset_index().sort_values(["l_returnflag", "l_linestatus"])

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
