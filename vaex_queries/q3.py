import numpy as np

from vaex_queries import utils

Q_NUM = 3


def q():
    var1 = var2 = np.datetime64("1995-03-15")
    var3 = "BUILDING"

    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds()
    line_item_ds()
    orders_ds()

    def query():
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        customer_ds = customer_ds()
        line_item_ds = line_item_ds()
        orders_ds = orders_ds()

        lineitem_filtered = line_item_ds[
            ["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"]
        ]
        orders_filtered = orders_ds[
            ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"]
        ]
        customer_filtered = customer_ds[["c_mktsegment", "c_custkey"]]
        lsel = lineitem_filtered.l_shipdate > var1
        osel = orders_filtered.o_orderdate < var2
        csel = customer_filtered.c_mktsegment == var3
        flineitem = lineitem_filtered[lsel]
        forders = orders_filtered[osel]
        forders = forders.sort("o_custkey")
        fcustomer = customer_filtered[csel]
        fcustomer = fcustomer.sort("c_custkey")

        jn1 = fcustomer.join(
            forders,
            left_on="c_custkey",
            right_on="o_custkey",
            how="inner",
            allow_duplication=True,
        )
        flineitem = flineitem.sort("l_orderkey")
        jn1 = jn1.sort("o_orderkey")
        jn2 = jn1.join(
            flineitem,
            left_on="o_orderkey",
            right_on="l_orderkey",
            how="inner",
            allow_duplication=True,
        )
        jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)

        total = (
            jn2.groupby(["l_orderkey", "o_orderdate", "o_shippriority"], as_index=False)
            .agg({"revenue": "sum"})
            .sort("values", ascending=False)
        )
        result_df = total[:10][
            ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]
        ]
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
