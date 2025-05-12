import polars as pl

store_sales = pl.scan_parquet("data/store_sales.parquet")
item = pl.scan_parquet("data/item.parquet")
date_dim = pl.scan_parquet("data/date_dim.parquet")

date_dim.join(store_sales, left_on="d_date_sk", right_on="ss_sold_date_sk").join(
    item, left_on="ss_item_sk", right_on="i_item_sk"
).filter((pl.col("i_manufact_id") == 128) & (pl.col("d_moy") == 11)).group_by(
    ["d_year", "i_brand_id", "i_brand"]
).agg(pl.col("ss_ext_sales_price").sum().alias("sum_agg")).rename(
    {"i_brand_id": "brand_id", "i_brand": "brand"}
).sort(
    by=["d_year", "sum_agg", "brand_id"],
    descending=[False, True, False],
).limit(100).collect()
