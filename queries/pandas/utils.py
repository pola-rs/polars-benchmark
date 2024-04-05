from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
from linetimer import CodeTimer

from queries.common_utils import check_query_result_pd, log_query_timing, on_second_call
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


def run_query(q_num: int, query: Callable[..., Any]) -> None:
    with CodeTimer(name=f"Run pandas query {q_num}", unit="s") as timer:
        result = query()

    if settings.run.log_timings:
        log_query_timing(
            solution="pandas",
            version=pd.__version__,
            query_number=q_num,
            time=timer.took,
        )

    if settings.run.check_results:
        if settings.scale_factor != 1:
            msg = f"cannot check results when scale factor is not 1, got {settings.scale_factor}"
            raise RuntimeError(msg)
        check_query_result_pd(result, q_num)

    if settings.run.show_results:
        print(result)
