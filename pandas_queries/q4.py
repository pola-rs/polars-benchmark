from datetime import datetime

import pyarrow.compute as pc

from pandas_queries import utils

Q_NUM = 4


def q():
    date1 = datetime(1993, 10, 1)
    date2 = datetime(1993, 7, 1)

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    columns_line = ["l_orderkey"]
    columns_orders = ["o_orderkey", "o_orderpriority"]
    kwargs_line = {"filters": pc.field("l_commitdate") < pc.field("l_receiptdate")}
    kwargs_orders = {
        "filters": (pc.field("o_orderdate") < date1)
        & (pc.field("o_orderdate") >= date2)
    }

    # first call one time to cache in case we don't include the IO times
    line_item_ds(columns=columns_line, kwargs=kwargs_line)
    orders_ds(columns=columns_orders, kwargs=kwargs_orders)

    def query():
        nonlocal line_item_ds
        nonlocal orders_ds
        flineitem = line_item_ds(columns=columns_line, kwargs=kwargs_line)
        forders = orders_ds(columns=columns_orders, kwargs=kwargs_orders)
        jn = forders[forders["o_orderkey"].isin(flineitem["l_orderkey"])]
        result_df = (
            jn.groupby("o_orderpriority", as_index=False)["o_orderkey"]
            .count()
            .sort_values(["o_orderpriority"])
            .rename(columns={"o_orderkey": "order_count"})
        )
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
