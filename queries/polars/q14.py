from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 14


def q() -> pl.LazyFrame:
    line_item_ds = utils.get_line_item_ds()
    part_ds = utils.get_part_ds()

    var_1 = date(1995, 9, 1)
    var_2 = date(1995, 10, 1)

    q_final = (
        line_item_ds.join(part_ds, left_on="l_partkey", right_on="p_partkey")
        .filter(pl.col("l_shipdate").is_between(var_1, var_2, closed="left"))
        .select(
            (
                100.00
                * pl.when(pl.col("p_type").str.contains("PROMO*"))
                .then(pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                .otherwise(0)
                .sum()
                / (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum()
            )
            .round(2)
            .alias("promo_revenue")
        )
    )

    return q_final


def main() -> None:
    args = utils.parse_parameters()
    query_plan = q()
    utils.run_query(Q_NUM, query_plan, **vars(args))


if __name__ == "__main__":
    main()
