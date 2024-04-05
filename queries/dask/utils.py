from __future__ import annotations

from typing import TYPE_CHECKING, Any

import dask
import dask.dataframe as dd

from queries.common_utils import (
    check_query_result_pd,
    on_second_call,
    run_query_generic,
)
from settings import Settings

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from dask.dataframe.core import DataFrame

settings = Settings()

dask.config.set(scheduler="threads")


def read_ds(path: Path) -> DataFrame:
    if settings.run.file_type != "parquet":
        msg = f"unsupported file type: {settings.run.file_type!r}"
        raise ValueError(msg)

    if settings.run.include_io:
        return dd.read_parquet(path, dtype_backend="pyarrow")  # type: ignore[attr-defined,no-any-return]

    # TODO: Load into memory before returning the Dask DataFrame.
    # Code below is tripped up by date types
    # df = pd.read_parquet(path, dtype_backend="pyarrow")
    # return dd.from_pandas(df, npartitions=os.cpu_count())
    msg = "cannot run Dask starting from an in-memory representation"
    raise RuntimeError(msg)


@on_second_call
def get_line_item_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "lineitem.parquet")


@on_second_call
def get_orders_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "orders.parquet")


@on_second_call
def get_customer_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "customer.parquet")


@on_second_call
def get_region_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "region.parquet")


@on_second_call
def get_nation_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "nation.parquet")


@on_second_call
def get_supplier_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "supplier.parquet")


@on_second_call
def get_part_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "part.parquet")


@on_second_call
def get_part_supp_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "partsupp.parquet")


def run_query(query_number: int, query: Callable[..., Any]) -> None:
    run_query_generic(query, query_number, "dask", query_checker=check_query_result_pd)
