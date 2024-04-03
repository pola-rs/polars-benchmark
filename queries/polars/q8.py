from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 8


def q() -> pl.LazyFrame:
    part_ds = utils.get_part_ds()
    supplier_ds = utils.get_supplier_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    customer_ds = utils.get_customer_ds()
    nation_ds = utils.get_nation_ds()
    region_ds = utils.get_region_ds()

    n1 = nation_ds.select("n_nationkey", "n_regionkey")
    n2 = nation_ds.clone().select("n_nationkey", "n_name")

    q_final = (
        part_ds.join(line_item_ds, left_on="p_partkey", right_on="l_partkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .join(customer_ds, left_on="o_custkey", right_on="c_custkey")
        .join(n1, left_on="c_nationkey", right_on="n_nationkey")
        .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("r_name") == "AMERICA")
        .join(n2, left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("o_orderdate").is_between(date(1995, 1, 1), date(1996, 12, 31)))
        .filter(pl.col("p_type") == "ECONOMY ANODIZED STEEL")
        .select(
            pl.col("o_orderdate").dt.year().alias("o_year"),
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume"),
            pl.col("n_name").alias("nation"),
        )
        .with_columns(
            pl.when(pl.col("nation") == "BRAZIL")
            .then(pl.col("volume"))
            .otherwise(0)
            .alias("_tmp")
        )
        .group_by("o_year")
        .agg((pl.sum("_tmp") / pl.sum("volume")).round(2).alias("mkt_share"))
        .sort("o_year")
    )

    return q_final


def main() -> None:
    args = utils.parse_parameters()
    query_plan = q()
    utils.run_query(Q_NUM, query_plan, **vars(args))


if __name__ == "__main__":
    main()
