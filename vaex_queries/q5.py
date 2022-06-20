import datetime

import numpy as np

from vaex_queries import utils

Q_NUM = 5


def q():
    date1 = np.datetime64("1994-01-01")
    date2 = np.datetime64("1995-01-01")

    region_ds = utils.get_region_ds
    nation_ds = utils.get_nation_ds
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    region_ds()
    nation_ds()
    customer_ds()
    line_item_ds()
    orders_ds()
    supplier_ds()

    def query():
        nonlocal region_ds
        nonlocal nation_ds
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal supplier_ds

        region_ds = region_ds()
        nation_ds = nation_ds()
        customer_ds = customer_ds()
        line_item_ds = line_item_ds()
        orders_ds = orders_ds()
        supplier_ds = supplier_ds()

        rsel = region_ds.r_name == "ASIA"
        osel = (orders_ds.o_orderdate >= date1) & (orders_ds.o_orderdate < date2)
        forders = orders_ds[osel]
        fregion = region_ds[rsel]
        # see: https://github.com/vaexio/vaex/issues/1319
        fregion = fregion.sort("r_regionkey")

        jn1 = fregion.join(
            nation_ds,
            left_on="r_regionkey",
            right_on="n_regionkey",
            how="inner",
            allow_duplication=True,
        )
        jn2 = jn1.join(
            customer_ds,
            left_on="n_nationkey",
            right_on="c_nationkey",
            how="inner",
            allow_duplication=True,
        )
        jn3 = jn2.join(
            forders,
            left_on="c_custkey",
            right_on="o_custkey",
            how="inner",
            allow_duplication=True,
        )
        jn4 = jn3.join(
            line_item_ds,
            left_on="o_orderkey",
            right_on="l_orderkey",
            how="inner",
            allow_duplication=True,
        )
        jn5 = supplier_ds.join(
            jn4,
            left_on=["s_suppkey", "s_nationkey"],
            right_on=["l_suppkey", "n_nationkey"],
            how="inner",
            allow_duplication=True,
        )
        jn5["revenue"] = jn5.l_extendedprice * (1.0 - jn5.l_discount)
        gb = jn5.groupby("n_name").agg({"revenue": "sum"})
        result_df = gb.sort("revenue", ascending=False)
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
