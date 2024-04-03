import polars as pl

from queries.polars import utils

Q_NUM = 16


def q() -> pl.LazyFrame:
    part_supp_ds = utils.get_part_supp_ds()
    part_ds = utils.get_part_ds()
    supplier_ds = (
        utils.get_supplier_ds()
        .filter(pl.col("s_comment").str.contains(".*Customer.*Complaints.*"))
        .select(pl.col("s_suppkey"), pl.col("s_suppkey").alias("ps_suppkey"))
    )

    var_1 = "Brand#45"

    q_final = (
        part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .filter(pl.col("p_brand") != var_1)
        .filter(pl.col("p_type").str.contains("MEDIUM POLISHED*").not_())
        .filter(pl.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey", how="left")
        .filter(pl.col("ps_suppkey_right").is_null())
        .group_by("p_brand", "p_type", "p_size")
        .agg(pl.col("ps_suppkey").n_unique().alias("supplier_cnt"))
        .sort(
            by=["supplier_cnt", "p_brand", "p_type", "p_size"],
            descending=[True, False, False, False],
        )
    )

    return q_final


def main() -> None:
    args = utils.parse_parameters()
    query_plan = q()
    utils.run_query(Q_NUM, query_plan, **vars(args))


if __name__ == "__main__":
    main()
