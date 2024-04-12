import os
from functools import partial

import polars as pl

from queries.common_utils import check_query_result_pl, run_query_generic
from settings import Settings

settings = Settings()

os.environ["POLARS_NO_STREAMING_GROUPBY"] = str(
    int(not settings.run.polars_streaming_groupby)
)


def _scan_ds(table_name: str) -> pl.LazyFrame:
    path = settings.dataset_base_dir / f"{table_name}.{settings.run.file_type}"

    if settings.run.file_type == "parquet":
        scan = pl.scan_parquet(path)
    elif settings.run.file_type == "feather":
        scan = pl.scan_ipc(path)
    elif settings.run.file_type == "csv":
        scan = pl.scan_csv(path, try_parse_dates=True)
    else:
        msg = f"unsupported file type: {settings.run.file_type!r}"
        raise ValueError(msg)

    if settings.run.include_io:
        return scan
    else:
        return scan.collect().rechunk().lazy()


def get_line_item_ds() -> pl.LazyFrame:
    return _scan_ds("lineitem")


def get_orders_ds() -> pl.LazyFrame:
    return _scan_ds("orders")


def get_customer_ds() -> pl.LazyFrame:
    return _scan_ds("customer")


def get_region_ds() -> pl.LazyFrame:
    return _scan_ds("region")


def get_nation_ds() -> pl.LazyFrame:
    return _scan_ds("nation")


def get_supplier_ds() -> pl.LazyFrame:
    return _scan_ds("supplier")


def get_part_ds() -> pl.LazyFrame:
    return _scan_ds("part")


def get_part_supp_ds() -> pl.LazyFrame:
    return _scan_ds("partsupp")


def run_query(query_number: int, lf: pl.LazyFrame) -> None:
    streaming = settings.run.polars_streaming

    if settings.run.polars_show_plan:
        print(lf.explain(streaming=streaming))

    query = partial(lf.collect, streaming=streaming)
    run_query_generic(
        query, query_number, "polars", query_checker=check_query_result_pl
    )
