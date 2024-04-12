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
    # TODO: Load into memory before returning the Dask DataFrame.
    # Code below is tripped up by date types
    # df = pd.read_parquet(path, dtype_backend="pyarrow")
    # return dd.from_pandas(df, npartitions=os.cpu_count())
    if not settings.run.include_io:
        msg = "cannot run Dask starting from an in-memory representation"
        raise RuntimeError(msg)

    path_str = f"{path}.{settings.run.file_type}"
    if settings.run.file_type == "parquet":
        return dd.read_parquet(path_str, dtype_backend="pyarrow")  # type: ignore[attr-defined,no-any-return]
    elif settings.run.file_type == "csv":
        df = dd.read_csv(path_str, dtype_backend="pyarrow")  # type: ignore[attr-defined]
        for c in df.columns:
            if c.endswith("date"):
                df[c] = df[c].astype("date32[day][pyarrow]")
        return df  # type: ignore[no-any-return]
    else:
        msg = f"unsupported file type: {settings.run.file_type!r}"
        raise ValueError(msg)


@on_second_call
def get_line_item_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "lineitem")


@on_second_call
def get_orders_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "orders")


@on_second_call
def get_customer_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "customer")


@on_second_call
def get_region_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "region")


@on_second_call
def get_nation_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "nation")


@on_second_call
def get_supplier_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "supplier")


@on_second_call
def get_part_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "part")


@on_second_call
def get_part_supp_ds() -> DataFrame:
    return read_ds(settings.dataset_base_dir / "partsupp")


def run_query(query_number: int, query: Callable[..., Any]) -> None:
    run_query_generic(query, query_number, "dask", query_checker=check_query_result_pd)
