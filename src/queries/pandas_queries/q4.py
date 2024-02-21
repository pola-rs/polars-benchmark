from datetime import datetime

from pandas_queries import utils

Q_NUM = 4


def q():
    date1 = datetime(1993, 10, 1)
    date2 = datetime(1993, 7, 1)

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
