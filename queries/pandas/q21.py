from __future__ import annotations

import pandas as pd

from queries.pandas import utils

Q_NUM = 21


def q() -> None:
    lineitem_ds_fn = utils.get_line_item_ds
    nation_ds_fn = utils.get_nation_ds
    orders_ds_fn = utils.get_orders_ds
    supplier_ds_fn = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    lineitem_ds_fn()
    nation_ds_fn()
    orders_ds_fn()
    supplier_ds_fn()

    def query() -> pd.DataFrame:
        lineitem_ds = lineitem_ds_fn()
        nation_ds = nation_ds_fn()
        orders_ds = orders_ds_fn()
        supplier_ds = supplier_ds_fn()

        var1 = "SAUDI ARABIA"

        # Find orders with multiple suppliers
        supp_per_order = lineitem_ds.groupby("l_orderkey", as_index=False).agg(
            n_supp_by_order=pd.NamedAgg(column="l_suppkey", aggfunc="count")
        )
        multi_supp_orders = supp_per_order[supp_per_order["n_supp_by_order"] > 1]

        # Join with lineitem where receiptdate > commitdate
        late_lineitem = lineitem_ds[
            lineitem_ds["l_receiptdate"] > lineitem_ds["l_commitdate"]
        ]
        jn1 = multi_supp_orders.merge(late_lineitem, on="l_orderkey", how="inner")

        # Re-calculate suppliers per order for the late items
        supp_per_order2 = jn1.groupby("l_orderkey", as_index=False).agg(
            n_supp_by_order=pd.NamedAgg(column="l_suppkey", aggfunc="count")
        )

        # Join back with lineitem data
        jn2 = supp_per_order2.merge(jn1, on="l_orderkey")

        # Filter to orders where only one supplier was late
        jn2 = jn2[jn2["n_supp_by_order_x"] == 1]

        # Join with supplier, nation, and orders
        jn3 = jn2.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        jn4 = jn3.merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        jn5 = jn4.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")

        # Filter by nation and order status
        jn5 = jn5[jn5["n_name"] == var1]
        jn5 = jn5[jn5["o_orderstatus"] == "F"]

        # Group by supplier name and count
        gb = jn5.groupby("s_name", as_index=False)
        agg = gb.size()
        agg.columns = ["s_name", "numwait"]

        result_df = agg.sort_values(
            by=["numwait", "s_name"], ascending=[False, True]
        ).head(100)

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
