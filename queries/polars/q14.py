from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 14


def q(
    customer: None | pl.LazyFrame,
    lineitem: None | pl.LazyFrame,
    nation: None | pl.LazyFrame,
    orders: None | pl.LazyFrame,
    partsupp: None | pl.LazyFrame,
    supplier: None | pl.LazyFrame,
    region: None | pl.LazyFrame,
    part: None | pl.LazyFrame,
    **kwargs
) -> pl.LazyFrame:
    if lineitem is None:
        lineitem = utils.get_line_item_ds()
        part = utils.get_part_ds()

    var1 = date(1995, 9, 1)
    var2 = date(1995, 10, 1)

    return (
        lineitem.join(part, left_on="l_partkey", right_on="p_partkey")
        .filter(pl.col("l_shipdate").is_between(var1, var2, closed="left"))
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


if __name__ == "__main__":
    utils.run_query(Q_NUM, q())
