import polars as pl

from polars_queries import utils

Q_NUM = 19


def q():
    line_item_ds = utils.get_line_item_ds()
    part_ds = utils.get_part_ds()

    q_final = (
        part_ds.join(line_item_ds, left_on="p_partkey", right_on="l_partkey")
        .filter(pl.col("l_shipmode").is_in(["AIR", "AIR REG"]))
        .filter(pl.col("l_shipinstruct") == "DELIVER IN PERSON")
        .filter(
            (
                (pl.col("p_brand") == "Brand#12")
                & pl.col("p_container").is_in(
                    ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
                )
                & (pl.col("l_quantity") >= 1)
                & (pl.col("l_quantity") <= 11)
                & (pl.col("p_size") >= 1)
                & (pl.col("p_size") <= 5)
            )
            | (
                (pl.col("p_brand") == "Brand#23")
                & pl.col("p_container").is_in(
                    ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
                )
                & (pl.col("l_quantity") >= 10)
                & (pl.col("l_quantity") <= 20)
                & (pl.col("p_size") >= 1)
                & (pl.col("p_size") <= 10)
            )
            | (
                (pl.col("p_brand") == "Brand#34")
                & pl.col("p_container").is_in(
                    ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
                )
                & (pl.col("l_quantity") >= 20)
                & (pl.col("l_quantity") <= 30)
                & (pl.col("p_size") >= 1)
                & (pl.col("p_size") <= 15)
            )
        )
        .select(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
            .sum()
            .round(2)
            .alias("revenue")
        )
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
