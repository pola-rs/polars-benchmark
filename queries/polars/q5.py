from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 5


def q() -> pl.LazyFrame:
    var_1 = "ASIA"
    var_2 = date(1994, 1, 1)
    var_3 = date(1995, 1, 1)

    region_ds = utils.get_region_ds()
    nation_ds = utils.get_nation_ds()
    customer_ds = utils.get_customer_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    supplier_ds = utils.get_supplier_ds()

    q_final = (
        region_ds.join(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
        .join(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(
            supplier_ds,
            left_on=["l_suppkey", "n_nationkey"],
            right_on=["s_suppkey", "s_nationkey"],
        )
        .filter(pl.col("r_name") == var_1)
        .filter(pl.col("o_orderdate").is_between(var_2, var_3, closed="left"))
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .group_by("n_name")
        .agg(pl.sum("revenue"))
        .sort(by="revenue", descending=True)
    )

    return q_final


def main() -> None:
    args = utils.parse_parameters()
    query_plan = q()
    utils.run_query(Q_NUM, query_plan, **vars(args))


if __name__ == "__main__":
    main()
