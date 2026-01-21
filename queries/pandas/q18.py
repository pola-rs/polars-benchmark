from __future__ import annotations

import pandas as pd

from queries.pandas import utils

Q_NUM = 18


def q() -> None:
    customer_ds_fn = utils.get_customer_ds
    lineitem_ds_fn = utils.get_line_item_ds
    orders_ds_fn = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds_fn()
    lineitem_ds_fn()
    orders_ds_fn()

    def query() -> pd.DataFrame:
        customer_ds = customer_ds_fn()
        lineitem_ds = lineitem_ds_fn()
        orders_ds = orders_ds_fn()

        var1 = 300

        # Find orders with sum quantity > 300
        qty_by_order = lineitem_ds.groupby("l_orderkey", as_index=False).agg(
            sum_quantity=pd.NamedAgg(column="l_quantity", aggfunc="sum")
        )
        large_orders = qty_by_order[qty_by_order["sum_quantity"] > var1][["l_orderkey"]]

        # Semi join: keep only orders that are in large_orders
        jn1 = orders_ds.merge(large_orders, left_on="o_orderkey", right_on="l_orderkey")
        jn2 = jn1.merge(lineitem_ds, left_on="o_orderkey", right_on="l_orderkey")
        jn3 = jn2.merge(customer_ds, left_on="o_custkey", right_on="c_custkey")

        gb = jn3.groupby(
            [
                "c_name",
                "o_custkey",
                "o_orderkey",
                "o_orderdate",
                "o_totalprice",
            ],
            as_index=False,
        )
        agg = gb.agg(col6=pd.NamedAgg(column="l_quantity", aggfunc="sum"))

        result = agg.loc[
            :,
            [
                "c_name",
                "o_custkey",
                "o_orderkey",
                "o_orderdate",
                "o_totalprice",
                "col6",
            ],
        ]
        result = result.rename(
            columns={"o_custkey": "c_custkey", "o_orderdate": "o_orderdat"}
        )

        result_df = result.sort_values(
            by=["o_totalprice", "o_orderdat"], ascending=[False, True]
        ).head(100)

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
