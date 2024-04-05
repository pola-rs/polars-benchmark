from functools import partial
from pathlib import Path

import polars as pl

from queries.common_utils import check_query_result_pl, run_query_generic
from settings import Settings

settings = Settings()


def _scan_ds(path: Path) -> pl.LazyFrame:
    path_str = f"{path}.{settings.run.file_type}"
    if settings.run.file_type == "parquet":
        scan = pl.scan_parquet(path_str)
    elif settings.run.file_type == "feather":
        scan = pl.scan_ipc(path_str)
    else:
        msg = f"unsupported file type: {settings.run.file_type!r}"
        raise ValueError(msg)
    if settings.run.include_io:
        return scan
    return scan.collect().rechunk().lazy()


def get_line_item_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "lineitem")


def get_orders_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "orders")


def get_customer_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "customer")


def get_region_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "region")


def get_nation_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "nation")


def get_supplier_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "supplier")


def get_part_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "part")


def get_part_supp_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "partsupp")


def run_query(query_number: int, lf: pl.LazyFrame) -> None:
    streaming = settings.run.polars_streaming

    if settings.run.polars_show_plan:
        print(lf.explain(streaming=streaming))

    query = partial(lf.collect, streaming=streaming)
    run_query_generic(
        query, query_number, "polars", query_checker=check_query_result_pl
    )
