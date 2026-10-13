from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Any

import modin.pandas as pd

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

pd.options.mode.copy_on_write = True

# Ray (Modin's engine) won't start properly on macOS when object store > 2GB;
# value comes from `ray._private.ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT`
modin_memory = settings.run.modin_memory
if sys.platform == "darwin":
    MAC_OBJECT_STORE_LIMIT = 2_147_483_648
    modin_memory = min(modin_memory, MAC_OBJECT_STORE_LIMIT)

os.environ["MODIN_MEMORY"] = str(modin_memory)


def _read_ds(table_name: str) -> pd.DataFrame:
    path = get_table_path(table_name)

    if settings.run.io_type in ("parquet", "skip"):
        return pd.read_parquet(path, dtype_backend="pyarrow")
    elif settings.run.io_type == "csv":
        df = pd.read_csv(path, dtype_backend="pyarrow")
        # TODO: This is slow - we should use the known schema to read dates directly
        for c in df.columns:
            if c.endswith("date"):
                df[c] = df[c].astype("date32[day][pyarrow]")
        return df
    elif settings.run.io_type == "feather":
        return pd.read_feather(path, dtype_backend="pyarrow")
    else:
        msg = f"unsupported file type: {settings.run.io_type!r}"
        raise ValueError(msg)


@on_second_call
def get_line_item_ds() -> pd.DataFrame:
    return _read_ds("lineitem")


@on_second_call
def get_orders_ds() -> pd.DataFrame:
    return _read_ds("orders")


@on_second_call
def get_customer_ds() -> pd.DataFrame:
    return _read_ds("customer")


@on_second_call
def get_region_ds() -> pd.DataFrame:
    return _read_ds("region")


@on_second_call
def get_nation_ds() -> pd.DataFrame:
    return _read_ds("nation")


@on_second_call
def get_supplier_ds() -> pd.DataFrame:
    return _read_ds("supplier")


@on_second_call
def get_part_ds() -> pd.DataFrame:
    return _read_ds("part")


@on_second_call
def get_part_supp_ds() -> pd.DataFrame:
    return _read_ds("partsupp")


def run_query(query_number: int, query: Callable[..., Any]) -> None:
    run_query_generic(
        query,
        query_number,
        "modin",
        query_checker=lambda df, q: check_query_result_pd(df._to_pandas(), q),
    )
