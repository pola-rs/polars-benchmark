import polars as pl

from polars_queries import utils

Q_NUM = 17


def q():
    var_1 = "Brand#23"
    var_2 = "MED BOX"

    line_item_ds = utils.get_line_item_ds()
    part_ds = utils.get_part_ds()

    res_1 = (
        part_ds.filter(pl.col("p_brand") == var_1)
        .filter(pl.col("p_container") == var_2)
        .join(line_item_ds, how="left", left_on="p_partkey", right_on="l_partkey")
    ).cache()

    q_final = (
        res_1.group_by("p_partkey")
        .agg((0.2 * pl.col("l_quantity").mean()).alias("avg_quantity"))
        .select([pl.col("p_partkey").alias("key"), pl.col("avg_quantity")])
        .join(res_1, left_on="key", right_on="p_partkey")
        .filter(pl.col("l_quantity") < pl.col("avg_quantity"))
        .select((pl.col("l_extendedprice").sum() / 7.0).round(2).alias("avg_yearly"))
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
