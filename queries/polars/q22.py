import polars as pl

from queries.polars import utils

Q_NUM = 22


def q() -> None:
    orders = utils.get_orders_ds()
    customer = utils.get_customer_ds()

    q1 = (
        customer.with_columns(pl.col("c_phone").str.slice(0, 2).alias("cntrycode"))
        .filter(pl.col("cntrycode").str.contains("13|31|23|29|30|18|17"))
        .select("c_acctbal", "c_custkey", "cntrycode")
    )

    q2 = (
        q1.filter(pl.col("c_acctbal") > 0.0)
        .select(pl.col("c_acctbal").mean().alias("avg_acctbal"))
        .with_columns(pl.lit(1).alias("lit"))
    )

    q3 = orders.select(pl.col("o_custkey").unique()).with_columns(
        pl.col("o_custkey").alias("c_custkey")
    )

    q_final = (
        q1.join(q3, on="c_custkey", how="left")
        .filter(pl.col("o_custkey").is_null())
        .with_columns(pl.lit(1).alias("lit"))
        .join(q2, on="lit")
        .filter(pl.col("c_acctbal") > pl.col("avg_acctbal"))
        .group_by("cntrycode")
        .agg(
            pl.col("c_acctbal").count().alias("numcust"),
            pl.col("c_acctbal").sum().round(2).alias("totacctbal"),
        )
        .sort("cntrycode")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
