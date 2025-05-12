import polars as pl

store_returns = pl.scan_parquet("data/store_returns.parquet")
date_dim = pl.scan_parquet("data/date_dim.parquet")
store = pl.scan_parquet("data/store.parquet")
customer = pl.scan_parquet("data/customer.parquet")

customer_total_return = (
    store_returns.join(
        (date_dim.filter(pl.col("d_year") == 2000)),
        left_on="sr_returned_date_sk",
        right_on="d_date_sk",
    )
    .group_by(
        [
            pl.col("sr_customer_sk").alias("ctr_customer_sk"),
            pl.col("sr_store_sk").alias("ctr_store_sk"),
        ]
    )
    .agg(pl.col("sr_return_amt").cast(pl.Float64).sum().alias("ctr_total_return"))
)

customer_total_return.join(
    store.filter(pl.col("s_state") == "TN"),
    left_on="ctr_store_sk",
    right_on="s_store_sk",
).join(customer, left_on="ctr_customer_sk", right_on="c_customer_sk").with_columns(
    [pl.col("ctr_total_return").mean().over("ctr_store_sk").alias("store_avg_return")]
).filter(pl.col("ctr_total_return") > pl.col("store_avg_return") * 1.2).select(
    "c_customer_id"
).sort("c_customer_id").limit(100).collect()
