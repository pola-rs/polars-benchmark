from datetime import datetime

import pandas as pd

from dask_queries import utils

Q_NUM = 4


def q():
    date1 = pd.Timestamp(datetime.strptime("1993-10-01", "%Y-%m-%d").date())
    date2 = pd.Timestamp(datetime.strptime("1993-07-01", "%Y-%m-%d").date())

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()

    def query():
        nonlocal line_item_ds
        nonlocal orders_ds
        line_item_ds = line_item_ds()
        orders_ds = orders_ds()

        lsel = line_item_ds.l_commitdate < line_item_ds.l_receiptdate
        osel = (orders_ds.o_orderdate < date1) & (orders_ds.o_orderdate >= date2)
        flineitem = line_item_ds[lsel]
        forders = orders_ds[osel]
        forders = forders[["o_orderkey", "o_orderpriority"]]
        # jn = forders[forders["o_orderkey"].compute().isin(flineitem["l_orderkey"])] # doesn't support isin
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
        return result_df.rename({"o_orderkey": "order_count"}, axis=1)

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
