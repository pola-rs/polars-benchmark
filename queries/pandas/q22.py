from __future__ import annotations

import pandas as pd

from queries.pandas import utils

Q_NUM = 22


def q() -> None:
    customer_ds_fn = utils.get_customer_ds
    orders_ds_fn = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds_fn()
    orders_ds_fn()

    def query() -> pd.DataFrame:
        customer_ds = customer_ds_fn()
        orders_ds = orders_ds_fn()

        # Extract country code (first 2 chars of phone)
        customer_with_cntry = customer_ds.copy()
        customer_with_cntry["cntrycode"] = customer_with_cntry["c_phone"].str.slice(
            0, 2
        )

        # Filter by country codes
        filtered_customer = customer_with_cntry[
            customer_with_cntry["cntrycode"].str.match("13|31|23|29|30|18|17", na=False)
        ][["c_acctbal", "c_custkey", "cntrycode"]]

        # Calculate average account balance for positive balances
        avg_acctbal = filtered_customer[filtered_customer["c_acctbal"] > 0.0][
            "c_acctbal"
        ].mean()

        # Get unique customer keys from orders
        customers_with_orders = orders_ds[["o_custkey"]].drop_duplicates()

        # Left join to find customers without orders
        jn = filtered_customer.merge(
            customers_with_orders,
            left_on="c_custkey",
            right_on="o_custkey",
            how="left",
        )
        jn = jn[jn["o_custkey"].isna()]

        # Filter by account balance > average
        jn = jn[jn["c_acctbal"] > avg_acctbal]

        # Group by country code
        gb = jn.groupby("cntrycode", as_index=False)
        agg = gb.agg(
            numcust=pd.NamedAgg(column="c_acctbal", aggfunc="count"),
            totacctbal=pd.NamedAgg(column="c_acctbal", aggfunc="sum"),
        )

        result_df = agg.sort_values("cntrycode")

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
