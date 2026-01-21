from __future__ import annotations

import pandas as pd

from queries.pandas import utils

Q_NUM = 20


def q() -> None:
    lineitem_ds_fn = utils.get_line_item_ds
    nation_ds_fn = utils.get_nation_ds
    part_ds_fn = utils.get_part_ds
    part_supp_ds_fn = utils.get_part_supp_ds
    supplier_ds_fn = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    lineitem_ds_fn()
    nation_ds_fn()
    part_ds_fn()
    part_supp_ds_fn()
    supplier_ds_fn()

    def query() -> pd.DataFrame:
        lineitem_ds = lineitem_ds_fn()
        nation_ds = nation_ds_fn()
        part_ds = part_ds_fn()
        part_supp_ds = part_supp_ds_fn()
        supplier_ds = supplier_ds_fn()

        var1 = pd.Timestamp("1994-01-01")
        var2 = pd.Timestamp("1995-01-01")
        var3 = "CANADA"
        var4 = "forest"

        # Aggregate lineitem by partkey and suppkey
        filtered_lineitem = lineitem_ds[
            (lineitem_ds["l_shipdate"] >= var1) & (lineitem_ds["l_shipdate"] < var2)
        ]
        qty_agg = filtered_lineitem.groupby(
            ["l_partkey", "l_suppkey"], as_index=False
        ).agg(sum_quantity=pd.NamedAgg(column="l_quantity", aggfunc="sum"))
        qty_agg["sum_quantity"] = qty_agg["sum_quantity"] * 0.5

        # Filter nation
        filtered_nation = nation_ds[nation_ds["n_name"] == var3]

        # Filter parts starting with "forest"
        filtered_part = part_ds[part_ds["p_name"].str.startswith(var4)][
            ["p_partkey"]
        ].drop_duplicates()

        # Join partsupp with filtered parts
        jn1 = filtered_part.merge(
            part_supp_ds, left_on="p_partkey", right_on="ps_partkey"
        )

        # Join with quantity aggregation
        jn2 = jn1.merge(
            qty_agg,
            left_on=["ps_suppkey", "p_partkey"],
            right_on=["l_suppkey", "l_partkey"],
        )

        # Filter by availqty > sum_quantity
        jn2 = jn2[jn2["ps_availqty"] > jn2["sum_quantity"]]

        # Get unique suppliers
        unique_suppliers = jn2[["ps_suppkey"]].drop_duplicates()

        # Join with supplier and nation
        jn3 = unique_suppliers.merge(
            supplier_ds, left_on="ps_suppkey", right_on="s_suppkey"
        )
        jn4 = jn3.merge(filtered_nation, left_on="s_nationkey", right_on="n_nationkey")

        result = jn4.loc[:, ["s_name", "s_address"]]

        result_df = result.sort_values("s_name")

        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
