from __future__ import annotations

from typing import TYPE_CHECKING, Any

import dask
import dask.dataframe as dd

from queries.common_utils import (
    check_query_result_pd,
    get_table_path,
    on_second_call,
    run_query_generic,
)
from settings import Settings

if TYPE_CHECKING:
    from collections.abc import Callable

    from dask.dataframe import DataFrame

settings = Settings()

dask.config.set(scheduler="threads")


def read_ds(table_name: str) -> DataFrame:
    if settings.run.io_type == "skip":
        # TODO: Load into memory before returning the Dask DataFrame.
        # Code below is tripped up by date types
        # df = pd.read_parquet(path, dtype_backend="pyarrow")
        # return dd.from_pandas(df, npartitions=os.cpu_count())
        msg = "cannot run Dask starting from an in-memory representation"
        raise RuntimeError(msg)

    path = get_table_path(table_name)

    if settings.run.io_type == "parquet":
        return dd.read_parquet(path, dtype_backend="pyarrow")  # type: ignore[no-any-return]
    elif settings.run.io_type == "csv":
        df = dd.read_csv(path, dtype_backend="pyarrow")
        for c in df.columns:
            if c.endswith("date"):
                df[c] = df[c].astype("date32[day][pyarrow]")
        return df  # type: ignore[no-any-return]
    else:
        msg = f"unsupported file type: {settings.run.io_type!r}"
        raise ValueError(msg)


@on_second_call
def get_line_item_ds() -> DataFrame:
    return read_ds("lineitem")


@on_second_call
def get_orders_ds() -> DataFrame:
    return read_ds("orders")


@on_second_call
def get_customer_ds() -> DataFrame:
    return read_ds("customer")


@on_second_call
def get_region_ds() -> DataFrame:
    return read_ds("region")


@on_second_call
def get_nation_ds() -> DataFrame:
    return read_ds("nation")


@on_second_call
def get_supplier_ds() -> DataFrame:
    return read_ds("supplier")


@on_second_call
def get_part_ds() -> DataFrame:
    return read_ds("part")


@on_second_call
def get_part_supp_ds() -> DataFrame:
    return read_ds("partsupp")


def run_query(query_number: int, query: Callable[..., Any]) -> None:
    run_query_generic(query, query_number, "dask", query_checker=check_query_result_pd)
