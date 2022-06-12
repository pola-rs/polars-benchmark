import datetime

from linetimer import CodeTimer, linetimer

from pandas_queries import pandas_tpch_utils

Q_NUM = 5


@linetimer(name=f"Overall execution of Query {Q_NUM}", unit="s")
def q():
    date1 = datetime.datetime.strptime("1994-01-01", "%Y-%m-%d").date()
    date2 = datetime.datetime.strptime("1995-01-01", "%Y-%m-%d").date()

    region_ds = pandas_tpch_utils.get_region_ds()
    nation_ds = pandas_tpch_utils.get_nation_ds()
    customer_ds = pandas_tpch_utils.get_customer_ds()
    line_item_ds = pandas_tpch_utils.get_line_item_ds()
    orders_ds = pandas_tpch_utils.get_orders_ds()
    supplier_ds = pandas_tpch_utils.get_supplier_ds()

    with CodeTimer(name=f"Get result of Query {Q_NUM}", unit="s"):
        rsel = region_ds.r_name == "ASIA"
        osel = (orders_ds.o_orderdate >= date1) & (orders_ds.o_orderdate < date2)
        forders = orders_ds[osel]
        fregion = region_ds[rsel]
        jn1 = fregion.merge(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
        jn2 = jn1.merge(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
        jn3 = jn2.merge(forders, left_on="c_custkey", right_on="o_custkey")
        jn4 = jn3.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        jn5 = supplier_ds.merge(
            jn4,
            left_on=["s_suppkey", "s_nationkey"],
            right_on=["l_suppkey", "n_nationkey"],
        )
        jn5["revenue"] = jn5.l_extendedprice * (1.0 - jn5.l_discount)
        gb = jn5.groupby("n_name", as_index=False)["revenue"].sum()
        result_df = gb.sort_values("revenue", ascending=False)
        print(result_df.head(10))

    pandas_tpch_utils.test_results(Q_NUM, result_df)


if __name__ == "__main__":
    q()
