import timeit
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
from linetimer import CodeTimer, linetimer
from pandas.api.types import is_string_dtype
from pandas.testing import assert_series_equal

from queries.common_utils import (
    DATASET_BASE_DIR,
    FILE_TYPE,
    LOG_TIMINGS,
    append_row,
    on_second_call,
    settings,
)

pd.options.mode.copy_on_write = True


def _read_ds(path: Path) -> pd.DataFrame:
    path_str = f"{path}.{FILE_TYPE}"
    if FILE_TYPE == "parquet":
        return pd.read_parquet(path_str, dtype_backend="pyarrow")
    elif FILE_TYPE == "feather":
        return pd.read_feather(path_str, dtype_backend="pyarrow")
    else:
        msg = f"file type: {FILE_TYPE} not expected"
        raise ValueError(msg)


def test_results(query_number: int, result_df: pd.DataFrame) -> None:
    answer = _get_query_answer(query_number)

    for c, t in answer.dtypes.items():
        s1 = result_df[c]
        s2 = answer[c]

        if is_string_dtype(t):
            s1 = s1.apply(lambda x: x.strip())

        assert_series_equal(left=s1, right=s2, check_index=False, check_dtype=False)


def _get_query_answer(query_number: int) -> pd.DataFrame:
    file_name = f"q{query_number}.parquet"
    file_path = settings.paths.answers / file_name
    return pd.read_parquet(file_path, dtype_backend="pyarrow")


@on_second_call
def get_line_item_ds(base_dir: Path = DATASET_BASE_DIR) -> pd.DataFrame:
    return _read_ds(base_dir / "lineitem")


@on_second_call
def get_orders_ds(base_dir: Path = DATASET_BASE_DIR) -> pd.DataFrame:
    return _read_ds(base_dir / "orders")


@on_second_call
def get_customer_ds(base_dir: Path = DATASET_BASE_DIR) -> pd.DataFrame:
    return _read_ds(base_dir / "customer")


@on_second_call
def get_region_ds(base_dir: Path = DATASET_BASE_DIR) -> pd.DataFrame:
    return _read_ds(base_dir / "region")


@on_second_call
def get_nation_ds(base_dir: Path = DATASET_BASE_DIR) -> pd.DataFrame:
    return _read_ds(base_dir / "nation")


@on_second_call
def get_supplier_ds(base_dir: Path = DATASET_BASE_DIR) -> pd.DataFrame:
    return _read_ds(base_dir / "supplier")


@on_second_call
def get_part_ds(base_dir: Path = DATASET_BASE_DIR) -> pd.DataFrame:
    return _read_ds(base_dir / "part")


@on_second_call
def get_part_supp_ds(base_dir: Path = DATASET_BASE_DIR) -> pd.DataFrame:
    return _read_ds(base_dir / "partsupp")


def run_query(q_num: int, query: Callable[..., Any]) -> None:
    @linetimer(name=f"Overall execution of pandas Query {q_num}", unit="s")  # type: ignore[misc]
    def run() -> None:
        with CodeTimer(name=f"Get result of pandas Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            result = query()
            secs = timeit.default_timer() - t0

        if LOG_TIMINGS:
            append_row(
                solution="pandas", version=pd.__version__, q=f"q{q_num}", secs=secs
            )
        else:
            test_results(q_num, result)

        if settings.print_query_output:
            print(result)

    run()
