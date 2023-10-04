import polars as pl

from polars_queries import utils

Q_NUM = 22


def q():
    orders_ds = utils.get_orders_ds()
    customer_ds = utils.get_customer_ds()

    res_1 = (
        customer_ds.with_columns(pl.col("c_phone").str.slice(0, 2).alias("cntrycode"))
        .filter(pl.col("cntrycode").str.contains("13|31|23|29|30|18|17"))
        .select(["c_acctbal", "c_custkey", "cntrycode"])
    )

    res_2 = (
        res_1.filter(pl.col("c_acctbal") > 0.0)
        .select(pl.col("c_acctbal").mean().alias("avg_acctbal"))
        .with_columns(pl.lit(1).alias("lit"))
    )

    res_3 = orders_ds.select(pl.col("o_custkey").unique()).with_columns(
        pl.col("o_custkey").alias("c_custkey")
    )

    q_final = (
        res_1.join(res_3, on="c_custkey", how="left")
        .filter(pl.col("o_custkey").is_null())
        .with_columns(pl.lit(1).alias("lit"))
        .join(res_2, on="lit")
        .filter(pl.col("c_acctbal") > pl.col("avg_acctbal"))
        .group_by("cntrycode")
        .agg(
            [
                pl.col("c_acctbal").count().alias("numcust"),
                pl.col("c_acctbal").sum().round(2).alias("totacctbal"),
            ]
        )
        .sort("cntrycode")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
