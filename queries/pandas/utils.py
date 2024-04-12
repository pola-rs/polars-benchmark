from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from queries.common_utils import (
    check_query_result_pd,
    on_second_call,
    run_query_generic,
)
from settings import Settings

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

settings = Settings()

pd.options.mode.copy_on_write = True


def _read_ds(path: Path) -> pd.DataFrame:
    path_str = f"{path}.{settings.run.file_type}"
    if settings.run.file_type == "parquet":
        return pd.read_parquet(path_str, dtype_backend="pyarrow")
    elif settings.run.file_type == "csv":
        df = pd.read_csv(path_str, dtype_backend="pyarrow")
        for c in df.columns:
            if c.endswith("date"):
                df[c] = df[c].astype("date32[day][pyarrow]")  # type: ignore[call-overload]
        return df
    elif settings.run.file_type == "feather":
        return pd.read_feather(path_str, dtype_backend="pyarrow")
    else:
        msg = f"unsupported file type: {settings.run.file_type!r}"
        raise ValueError(msg)


@on_second_call
def get_line_item_ds() -> pd.DataFrame:
    return _read_ds(settings.dataset_base_dir / "lineitem")


@on_second_call
def get_orders_ds() -> pd.DataFrame:
    return _read_ds(settings.dataset_base_dir / "orders")


@on_second_call
def get_customer_ds() -> pd.DataFrame:
    return _read_ds(settings.dataset_base_dir / "customer")


@on_second_call
def get_region_ds() -> pd.DataFrame:
    return _read_ds(settings.dataset_base_dir / "region")


@on_second_call
def get_nation_ds() -> pd.DataFrame:
    return _read_ds(settings.dataset_base_dir / "nation")


@on_second_call
def get_supplier_ds() -> pd.DataFrame:
    return _read_ds(settings.dataset_base_dir / "supplier")


@on_second_call
def get_part_ds() -> pd.DataFrame:
    return _read_ds(settings.dataset_base_dir / "part")


@on_second_call
def get_part_supp_ds() -> pd.DataFrame:
    return _read_ds(settings.dataset_base_dir / "partsupp")


def run_query(query_number: int, query: Callable[..., Any]) -> None:
    run_query_generic(
        query, query_number, "pandas", query_checker=check_query_result_pd
    )
