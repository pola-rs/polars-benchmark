from os.path import join

import polars as pl

__default_base_dir = "tables_scale_1"


def __scan_parquet_ds(path: str):
    return pl.scan_parquet(path)


def get_line_item_ds(base_dir: str = __default_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "lineitem.parquet"))


def get_orders_ds(base_dir: str = __default_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "orders.parquet"))


def get_customer_ds(base_dir: str = __default_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "customer.parquet"))
