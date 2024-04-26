import polars as pl

from queries.polars import utils

Q_NUM = 18


def q() -> None:
    customer = utils.get_customer_ds()
    lineitem = utils.get_line_item_ds()
    orders = utils.get_orders_ds()

    var1 = 300

    q_final = (
        lineitem.group_by("l_orderkey")
        .agg(pl.col("l_quantity").sum().alias("sum_quantity"))
        .filter(pl.col("sum_quantity") > var1)
        .select(pl.col("l_orderkey").alias("key"), pl.col("sum_quantity"))
        .join(orders, left_on="key", right_on="o_orderkey")
        .join(lineitem, left_on="key", right_on="l_orderkey")
        .join(customer, left_on="o_custkey", right_on="c_custkey")
        .group_by("c_name", "o_custkey", "key", "o_orderdate", "o_totalprice")
        .agg(pl.col("l_quantity").sum().alias("col6"))
        .select(
            pl.col("c_name"),
            pl.col("o_custkey").alias("c_custkey"),
            pl.col("key").alias("o_orderkey"),
            pl.col("o_orderdate").alias("o_orderdat"),
            pl.col("o_totalprice"),
            pl.col("col6"),
        )
        .sort(by=["o_totalprice", "o_orderdat"], descending=[True, False])
        .head(100)
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
