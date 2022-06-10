import datetime

from linetimer import CodeTimer
from linetimer import linetimer

from pandas_queries import pandas_tpch_utils

Q_NUM = 3


@linetimer(name=f"Overall execution of Query {Q_NUM}", unit='s')
def q():
    var1 = var2 = datetime.datetime.strptime('1995-03-15', '%Y-%m-%d').date()
    var3 = "BUILDING"

    customer_ds = pandas_tpch_utils.get_customer_ds()
    line_item_ds = pandas_tpch_utils.get_line_item_ds()
    orders_ds = pandas_tpch_utils.get_orders_ds()

    with CodeTimer(name=f"Get result of Query {Q_NUM}", unit='s'):
        lineitem_filtered = line_item_ds.loc[:, ["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"]]
        orders_filtered = orders_ds.loc[:, ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"]]
        customer_filtered = customer_ds.loc[:, ["c_mktsegment", "c_custkey"]]
        lsel = lineitem_filtered.l_shipdate > var1
        osel = orders_filtered.o_orderdate < var2
        csel = customer_filtered.c_mktsegment == var3
        flineitem = lineitem_filtered[lsel]
        forders = orders_filtered[osel]
        fcustomer = customer_filtered[csel]
        jn1 = fcustomer.merge(forders, left_on="c_custkey", right_on="o_custkey")
        jn2 = jn1.merge(flineitem, left_on="o_orderkey", right_on="l_orderkey")
        jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)

        total = (
            jn2.groupby(["l_orderkey", "o_orderdate", "o_shippriority"], as_index=False)["revenue"]
            .sum()
            .sort_values(["revenue"], ascending=False)
        )
        result_df = total[:10].reset_index(drop=True).loc[:, ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]]
        print(result_df.head(10))

    pandas_tpch_utils.test_results(Q_NUM, result_df)


if __name__ == '__main__':
    q()
