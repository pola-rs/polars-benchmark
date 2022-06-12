from datetime import datetime

import polars as pl

from polars_queries import polars_tpch_utils

Q_NUM = 1


def q():
    q = polars_tpch_utils.get_line_item_ds()
    q_final = (
        q.filter(pl.col("l_shipdate") <= datetime(1998, 9, 2))
        .groupby(["l_returnflag", "l_linestatus"])
        .agg(
            [
                pl.sum("l_quantity").alias("sum_qty"),
                pl.sum("l_extendedprice").alias("sum_base_price"),
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                .sum()
                .alias("sum_disc_price"),
                (
                    pl.col("l_extendedprice")
                    * (1.0 - pl.col("l_discount") * (1.0 - pl.col("l_tax")))
                )
                .sum()
                .alias("sum_charge"),
                pl.mean("l_quantity").alias("avg_qty"),
                pl.mean("l_extendedprice").alias("avg_price"),
                pl.mean("l_discount").alias("avg_disc"),
                pl.count().alias("count_order"),
            ],
        )
        .sort(["l_returnflag", "l_linestatus"])
    )

    polars_tpch_utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
