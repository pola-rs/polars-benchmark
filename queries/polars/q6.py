from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 6


def q() -> pl.LazyFrame:
    var_1 = date(1994, 1, 1)
    var_2 = date(1995, 1, 1)
    var_3 = 24

    line_item_ds = utils.get_line_item_ds()

    q_final = (
        line_item_ds.filter(
            pl.col("l_shipdate").is_between(var_1, var_2, closed="left")
        )
        .filter(pl.col("l_discount").is_between(0.05, 0.07))
        .filter(pl.col("l_quantity") < var_3)
        .with_columns(
            (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
        )
        .select(pl.sum("revenue"))
    )

    return q_final


def main() -> None:
    args = utils.parse_parameters()
    query_plan = q()
    utils.run_query(Q_NUM, query_plan, **vars(args))


if __name__ == "__main__":
    main()
