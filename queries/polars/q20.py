from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 20


def q() -> pl.LazyFrame:
    line_item_ds = utils.get_line_item_ds()
    nation_ds = utils.get_nation_ds()
    supplier_ds = utils.get_supplier_ds()
    part_ds = utils.get_part_ds()
    part_supp_ds = utils.get_part_supp_ds()

    var_1 = date(1994, 1, 1)
    var_2 = date(1995, 1, 1)
    var_3 = "CANADA"
    var_4 = "forest"

    res_1 = (
        line_item_ds.filter(
            pl.col("l_shipdate").is_between(var_1, var_2, closed="left")
        )
        .group_by("l_partkey", "l_suppkey")
        .agg((pl.col("l_quantity").sum() * 0.5).alias("sum_quantity"))
    )
    res_2 = nation_ds.filter(pl.col("n_name") == var_3)
    res_3 = supplier_ds.join(res_2, left_on="s_nationkey", right_on="n_nationkey")

    q_final = (
        part_ds.filter(pl.col("p_name").str.starts_with(var_4))
        .select(pl.col("p_partkey").unique())
        .join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .join(
            res_1,
            left_on=["ps_suppkey", "p_partkey"],
            right_on=["l_suppkey", "l_partkey"],
        )
        .filter(pl.col("ps_availqty") > pl.col("sum_quantity"))
        .select(pl.col("ps_suppkey").unique())
        .join(res_3, left_on="ps_suppkey", right_on="s_suppkey")
        .with_columns(pl.col("s_address").str.strip_chars())
        .select("s_name", "s_address")
        .sort("s_name")
    )

    return q_final


def main() -> None:
    args = utils.parse_parameters()
    query_plan = q()
    utils.run_query(Q_NUM, query_plan, **vars(args))


if __name__ == "__main__":
    main()
