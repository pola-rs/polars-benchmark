from __future__ import annotations

import pandas as pd

from queries.pandas import utils

Q_NUM = 13


def q() -> None:
    customer_ds_fn = utils.get_customer_ds
    orders_ds_fn = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds_fn()
    orders_ds_fn()

    def query() -> pd.DataFrame:
        customer_ds = customer_ds_fn()
        orders_ds = orders_ds_fn()

        var1 = "special"
        var2 = "requests"

        filtered_orders = orders_ds[
            ~orders_ds["o_comment"].str.contains(
                f"{var1}.*{var2}", regex=True, na=False
            )
        ]

        jn = customer_ds.merge(
            filtered_orders,
            left_on="c_custkey",
            right_on="o_custkey",
            how="left",
        )

        gb1 = jn.groupby("c_custkey", as_index=False)
        agg1 = gb1.agg(c_count=pd.NamedAgg(column="o_orderkey", aggfunc="count"))

        gb2 = agg1.groupby("c_count", as_index=False)
        agg2 = gb2.size()
        agg2.columns = ["c_count", "custdist"]

        result_df = agg2.sort_values(
            by=["custdist", "c_count"], ascending=[False, False]
        )

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
