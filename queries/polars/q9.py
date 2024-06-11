import polars as pl

from queries.polars import utils

Q_NUM = 9


def q() -> None:
    lineitem = utils.get_line_item_ds()
    nation = utils.get_nation_ds()
    orders = utils.get_orders_ds()
    part = utils.get_part_ds()
    partsupp = utils.get_part_supp_ds()
    supplier = utils.get_supplier_ds()

    q_final = (
        part.join(partsupp, left_on="p_partkey", right_on="ps_partkey")
        .join(supplier, left_on="ps_suppkey", right_on="s_suppkey")
        .join(
            lineitem,
            left_on=["p_partkey", "ps_suppkey"],
            right_on=["l_partkey", "l_suppkey"],
        )
        .join(orders, left_on="l_orderkey", right_on="o_orderkey")
        .join(nation, left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("p_name").str.contains("green"))
        .select(
            pl.col("n_name").alias("nation"),
            pl.col("o_orderdate").dt.year().alias("o_year"),
            (
                pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
                - pl.col("ps_supplycost") * pl.col("l_quantity")
            ).alias("amount"),
        )
        .group_by("nation", "o_year")
        .agg(pl.sum("amount").round(2).alias("sum_profit"))
        .sort(by=["nation", "o_year"], descending=[False, True])
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
