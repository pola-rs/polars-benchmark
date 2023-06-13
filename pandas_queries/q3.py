from datetime import datetime

import pyarrow.compute as pc

from pandas_queries import utils

Q_NUM = 3


def q():
    var1 = var2 = datetime(1995, 3, 15)
    var3 = "BUILDING"

    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    columns_line = ["l_orderkey", "l_extendedprice", "l_discount"]
    columns_order = ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"]
    columns_customer = ["c_custkey"]
    kwargs_lineitem = {"filters": pc.field("l_shipdate") > var1}
    kwargs_orders = {"filters": pc.field("o_orderdate") < var2}
    kwargs_customers = {"filters": pc.field("c_mktsegment") == var3}

    # first call one time to cache in case we don't include the IO times
    customer_ds(columns=columns_customer, kwargs=kwargs_customers)
    line_item_ds(columns=columns_line, kwargs=kwargs_lineitem)
    orders_ds(columns=columns_order, kwargs=kwargs_orders)

    def query():
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        customer_ds = customer_ds(columns=columns_customer, kwargs=kwargs_customers)
        line_item_ds = line_item_ds(columns=columns_line, kwargs=kwargs_lineitem)
        orders_ds = orders_ds(columns=columns_order, kwargs=kwargs_orders)

        flineitem = line_item_ds
        forders = orders_ds
        fcustomer = customer_ds
        jn1 = forders[forders.o_custkey.isin(fcustomer.c_custkey)]
        flineitem = flineitem[flineitem.l_orderkey.isin(jn1.o_orderkey)]
        jn2 = jn1.merge(flineitem, left_on="o_orderkey", right_on="l_orderkey")
        jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)

        total = (
            jn2.groupby(
                ["l_orderkey", "o_orderdate", "o_shippriority"], as_index=False
            )["revenue"]
            .sum()
            .sort_values(["revenue"], ascending=False)
        )
        result_df = total[:10].loc[
            :, ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]
        ]
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
