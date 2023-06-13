import datetime

import pyarrow.compute as pc

from pandas_queries import utils

Q_NUM = 5


def q():
    date1 = datetime.datetime.strptime("1994-01-01", "%Y-%m-%d")
    date2 = datetime.datetime.strptime("1995-01-01", "%Y-%m-%d")

    region_ds = utils.get_region_ds
    nation_ds = utils.get_nation_ds
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds
    columns_nations = ["n_regionkey", "n_nationkey", "n_name"]
    columns_region = ["r_regionkey", "r_name"]
    columns_customer = ["c_nationkey", "c_custkey"]
    columns_orders = ["o_custkey", "o_orderkey"]
    columns_line = ["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"]
    columns_supp = ["s_suppkey", "s_nationkey"]
    kwargs_orders = {
        "filters": (pc.field("o_orderdate") >= date1)
        & (pc.field("o_orderdate") < date2)
    }

    # first call one time to cache in case we don't include the IO times
    region_ds(columns=columns_region)
    nation_ds(columns=columns_nations)
    customer_ds(columns=columns_customer)
    line_item_ds(columns=columns_line)
    orders_ds(columns=columns_orders, kwargs=kwargs_orders)
    supplier_ds(columns=columns_supp)

    def query():
        nonlocal region_ds
        nonlocal nation_ds
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal supplier_ds

        region_ds = region_ds(columns=columns_region)
        nation_ds = nation_ds(columns=columns_nations)
        customer_ds = customer_ds(columns=columns_customer)
        line_item_ds = line_item_ds(columns=columns_line)
        orders_ds = orders_ds(columns=columns_orders, kwargs=kwargs_orders)
        supplier_ds = supplier_ds(columns=columns_supp)

        rsel = region_ds.r_name == "ASIA"
        forders = orders_ds
        fregion = region_ds[rsel]
        jn1 = fregion.merge(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
        jn2 = jn1.merge(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
        jn3 = jn2.merge(forders, left_on="c_custkey", right_on="o_custkey")

        line_item_ds = line_item_ds[line_item_ds.l_orderkey.isin(jn3.o_orderkey)]
        line_item_ds["revenue"] = line_item_ds.l_extendedprice * (
            1.0 - line_item_ds.l_discount
        )
        line_item_ds.drop(columns=["l_extendedprice", "l_discount"], inplace=True)
        jn4 = jn3.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        jn5 = supplier_ds.merge(
            jn4,
            left_on=["s_suppkey", "s_nationkey"],
            right_on=["l_suppkey", "n_nationkey"],
        )
        gb = jn5.groupby("n_name", as_index=False)["revenue"].sum()
        result_df = gb.sort_values("revenue", ascending=False)
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
