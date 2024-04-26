from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 14


def q() -> None:
    lineitem = utils.get_line_item_ds()
    part = utils.get_part_ds()

    var1 = date(1995, 9, 1)
    var2 = date(1995, 10, 1)

    q_final = (
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

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
