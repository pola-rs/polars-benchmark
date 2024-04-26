from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 20


def q() -> None:
    lineitem = utils.get_line_item_ds()
    nation = utils.get_nation_ds()
    part = utils.get_part_ds()
    partsupp = utils.get_part_supp_ds()
    supplier = utils.get_supplier_ds()

    var1 = date(1994, 1, 1)
    var2 = date(1995, 1, 1)
    var3 = "CANADA"
    var4 = "forest"

    q1 = (
        lineitem.filter(pl.col("l_shipdate").is_between(var1, var2, closed="left"))
        .group_by("l_partkey", "l_suppkey")
        .agg((pl.col("l_quantity").sum() * 0.5).alias("sum_quantity"))
    )
    q2 = nation.filter(pl.col("n_name") == var3)
    q3 = supplier.join(q2, left_on="s_nationkey", right_on="n_nationkey")

    q_final = (
        part.filter(pl.col("p_name").str.starts_with(var4))
        .select(pl.col("p_partkey").unique())
        .join(partsupp, left_on="p_partkey", right_on="ps_partkey")
        .join(
            q1,
            left_on=["ps_suppkey", "p_partkey"],
            right_on=["l_suppkey", "l_partkey"],
        )
        .filter(pl.col("ps_availqty") > pl.col("sum_quantity"))
        .select(pl.col("ps_suppkey").unique())
        .join(q3, left_on="ps_suppkey", right_on="s_suppkey")
        .select("s_name", "s_address")
        .sort("s_name")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
