from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

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

settings = Settings()

dask.config.set(scheduler="threads")


def read_ds(table_name: str) -> dd.DataFrame:
    if settings.run.io_type == "skip":
        # TODO: Load into memory before returning the Dask dd.DataFrame.
        # Code below is tripped up by date types
        # df = pd.read_parquet(path, dtype_backend="pyarrow")
        # return dd.from_pandas(df, npartitions=os.cpu_count())
        msg = "cannot run Dask starting from an in-memory representation"
        raise RuntimeError(msg)

    path = get_table_path(table_name)

    if settings.run.io_type == "parquet":
        return cast("dd.DataFrame", dd.read_parquet(path, dtype_backend="pyarrow"))
    elif settings.run.io_type == "csv":
        df: dd.DataFrame = dd.read_csv(path, dtype_backend="pyarrow")
        for c in df.columns:
            if c.endswith("date"):
                df[c] = df[c].astype("date32[day][pyarrow]")
        return df
    else:
        msg = f"unsupported file type: {settings.run.io_type!r}"
        raise ValueError(msg)


@on_second_call
def get_line_item_ds() -> dd.DataFrame:
    return read_ds("lineitem")


@on_second_call
def get_orders_ds() -> dd.DataFrame:
    return read_ds("orders")


@on_second_call
def get_customer_ds() -> dd.DataFrame:
    return read_ds("customer")


@on_second_call
def get_region_ds() -> dd.DataFrame:
    return read_ds("region")


@on_second_call
def get_nation_ds() -> dd.DataFrame:
    return read_ds("nation")


@on_second_call
def get_supplier_ds() -> dd.DataFrame:
    return read_ds("supplier")


@on_second_call
def get_part_ds() -> dd.DataFrame:
    return read_ds("part")


@on_second_call
def get_part_supp_ds() -> dd.DataFrame:
    return read_ds("partsupp")


def run_query(query_number: int, query: Callable[..., Any]) -> None:
    run_query_generic(query, query_number, "dask", query_checker=check_query_result_pd)
