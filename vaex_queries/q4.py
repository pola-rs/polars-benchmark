import numpy as np

from vaex_queries import utils

Q_NUM = 4


def q():
    date1 = np.datetime64("1993-10-01")
    date2 = np.datetime64("1993-07-01")
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()

    def drop_duplicates(df, columns=None):
        import vaex

        if columns is None:
            columns = df.get_column_names()
        if type(columns) is str:
            columns = [columns]
        return df.groupby(columns, agg={"__hidden_count": vaex.agg.count()}).drop(
            "__hidden_count"
        )

    def query():
        nonlocal line_item_ds
        nonlocal orders_ds
        line_item_ds = line_item_ds()
        orders_ds = orders_ds()

        lsel = line_item_ds.l_commitdate < line_item_ds.l_receiptdate
        osel = (orders_ds.o_orderdate < date1) & (orders_ds.o_orderdate >= date2)
        flineitem = line_item_ds[lsel]
        forders = orders_ds[osel]

        # see: https://github.com/vaexio/vaex/issues/1319
        forders = forders.sort("o_orderkey")

        jn = forders.join(
            flineitem,
            left_on="o_orderkey",
            right_on="l_orderkey",
            how="inner",
            allow_duplication=True,
        )
        # cannot finish this query because we cannot drop_duplicates by a subset
        jn = drop_duplicates(jn, columns=["o_orderkey"])
        [["o_orderpriority", "o_orderkey"]]

        result_df = (
            jn.groupby("o_orderpriority")
            .agg({"o_orderkey": "count"})
            .sort(["o_orderpriority"])
        )
        result_df["order_count"] = result_df["o_orderkey"]
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
