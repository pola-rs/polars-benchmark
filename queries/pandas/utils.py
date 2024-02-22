import timeit
from collections.abc import Callable
from pathlib import Path

import pandas as pd
from linetimer import CodeTimer, linetimer
from pandas.api.types import is_string_dtype
from pandas.core.frame import DataFrame as PandasDF
from pandas.testing import assert_series_equal

from queries.common_utils import (
    ANSWERS_BASE_DIR,
    DATASET_BASE_DIR,
    FILE_TYPE,
    LOG_TIMINGS,
    SHOW_RESULTS,
    append_row,
    on_second_call,
)

pd.options.mode.copy_on_write = True


def _read_ds(path: Path) -> PandasDF:
    path = f"{path}.{FILE_TYPE}"
    if FILE_TYPE == "parquet":
        return pd.read_parquet(path, dtype_backend="pyarrow")
    elif FILE_TYPE == "feather":
        return pd.read_feather(path, dtype_backend="pyarrow")
    else:
        msg = f"file type: {FILE_TYPE} not expected"
        raise ValueError(msg)


def get_query_answer(query: int, base_dir: str = ANSWERS_BASE_DIR) -> PandasDF:
    path = base_dir / f"q{query}.parquet"
    return pd.read_parquet(path, dtype_backend="pyarrow")


def test_results(q_num: int, result_df: PandasDF):
    with CodeTimer(name=f"Testing result of pandas Query {q_num}", unit="s"):
        answer = get_query_answer(q_num)

        for c, t in answer.dtypes.items():
            s1 = result_df[c]
            s2 = answer[c]

            if is_string_dtype(t):
                s1 = s1.apply(lambda x: x.strip())

            # TODO: Remove this cast
            if s2.dtype == "date32[day][pyarrow]":
                s2 = s2.astype("timestamp[us][pyarrow]")

            assert_series_equal(left=s1, right=s2, check_index=False, check_dtype=False)


@on_second_call
def get_line_item_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return _read_ds(Path(base_dir) / "lineitem")


@on_second_call
def get_orders_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return _read_ds(Path(base_dir) / "orders")


@on_second_call
def get_customer_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return _read_ds(Path(base_dir) / "customer")


@on_second_call
def get_region_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return _read_ds(Path(base_dir) / "region")


@on_second_call
def get_nation_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return _read_ds(Path(base_dir) / "nation")


@on_second_call
def get_supplier_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return _read_ds(Path(base_dir) / "supplier")


@on_second_call
def get_part_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return _read_ds(Path(base_dir) / "part")


@on_second_call
def get_part_supp_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return _read_ds(Path(base_dir) / "partsupp")


def run_query(q_num: int, query: Callable):
    @linetimer(name=f"Overall execution of pandas Query {q_num}", unit="s")
    def run():
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

        if SHOW_RESULTS:
            print(result)

    run()
