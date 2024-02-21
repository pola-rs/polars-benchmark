import polars as pl

from queries.polars import utils

Q_NUM = 13


def q():
    var_1 = "special"
    var_2 = "requests"

    customer_ds = utils.get_customer_ds()
    orders_ds = utils.get_orders_ds().filter(
        pl.col("o_comment").str.contains(f"{var_1}.*{var_2}").not_()
    )
    q_final = (
        customer_ds.join(
            orders_ds, left_on="c_custkey", right_on="o_custkey", how="left"
        )
        .group_by("c_custkey")
        .agg(pl.col("o_orderkey").count().alias("c_count"))
        .group_by("c_count")
        .len()
        .select(pl.col("c_count"), pl.col("len").alias("custdist"))
        .sort(by=["custdist", "c_count"], descending=[True, True])
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
