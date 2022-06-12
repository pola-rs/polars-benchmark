import datetime

from linetimer import CodeTimer, linetimer

from pandas_queries import pandas_tpch_utils

Q_NUM = 4


@linetimer(name=f"Overall execution of Query {Q_NUM}", unit="s")
def q():
    date1 = datetime.datetime.strptime("1993-10-01", "%Y-%m-%d").date()
    date2 = datetime.datetime.strptime("1993-07-01", "%Y-%m-%d").date()

    line_item_ds = pandas_tpch_utils.get_line_item_ds()
    orders_ds = pandas_tpch_utils.get_orders_ds()

    with CodeTimer(name=f"Get result of Query {Q_NUM}", unit="s"):
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
        print(result_df.head(10))

    pandas_tpch_utils.test_results(Q_NUM, result_df)


if __name__ == "__main__":
    q()
