# https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Sample%20querys.20.xml?embedded=true#d8087841bcc1c702b02b03b8ef02039a
import polars as pl

part = pl.scan_parquet(
    "tables_scale_1/part.parquet",
)

supplier = pl.scan_parquet(
    "tables_scale_1/supplier.parquet",
)

partsupp = pl.scan_parquet(
    "tables_scale_1/partsupp.parquet",
)

nation = pl.scan_parquet(
    "tables_scale_1/nation.parquet",
)

region = pl.scan_parquet(
    "tables_scale_1/region.parquet",
)

min_cost = (
    (
        partsupp.join(supplier, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation, left_on="s_nationkey", right_on="n_nationkey")
        .join(region, left_on="n_regionkey", right_on="r_regionkey")
        .filter((pl.col("r_name") == "EUROPE"))
        .select(pl.min("ps_supplycost"))
    )
    .collect()
    .to_series()
)

print(min_cost)

q = (
    part.join(partsupp, left_on="p_partkey", right_on="ps_partkey")
    .join(supplier, left_on="ps_suppkey", right_on="s_suppkey")
    .join(nation, left_on="s_nationkey", right_on="n_nationkey")
    .join(region, left_on="n_regionkey", right_on="r_regionkey")
)

q = q.filter(
    (pl.col("p_size") == 15)
    & (pl.col("p_type").str.contains(r".*BRASS"))
    & (pl.col("r_name") == "EUROPE")
    & (pl.col("ps_supplycost") == min_cost)
).select(
    [
        "ps_supplycost",
        "s_acctbal",
        "s_name",
        "n_name",
        "p_partkey",
        "p_mfgr",
        "s_address",
        "s_phone",
        "s_comment",
    ]
)

print(q.collect())
# print(nation.fetch(5), region.fetch(5))
