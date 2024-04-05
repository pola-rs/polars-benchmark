import timeit
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
from linetimer import CodeTimer, linetimer
from pandas.api.types import is_string_dtype
from pandas.core.frame import DataFrame as PandasDF
from pandas.testing import assert_series_equal

from queries.common_utils import log_query_timing, on_second_call
from settings import Settings

settings = Settings()

pd.options.mode.copy_on_write = True


def _read_ds(path: Path) -> PandasDF:
    path_str = f"{path}.{settings.run.file_type}"
    if settings.run.file_type == "parquet":
        return pd.read_parquet(path_str, dtype_backend="pyarrow")
    elif settings.run.file_type == "feather":
        return pd.read_feather(path_str, dtype_backend="pyarrow")
    else:
        msg = f"unsupported file type: {settings.run.file_type!r}"
        raise ValueError(msg)


def get_query_answer(query: int) -> PandasDF:
    path = settings.paths.answers / f"q{query}.parquet"
    return pd.read_parquet(path, dtype_backend="pyarrow")


def test_results(q_num: int, result_df: PandasDF) -> None:
    with CodeTimer(name=f"Testing result of pandas Query {q_num}", unit="s"):
        answer = get_query_answer(q_num)

        for c, t in answer.dtypes.items():
            s1 = result_df[c]
            s2 = answer[c]

            if is_string_dtype(t):
                s1 = s1.apply(lambda x: x.strip())

            assert_series_equal(left=s1, right=s2, check_index=False, check_dtype=False)


@on_second_call
def get_line_item_ds() -> PandasDF:
    return _read_ds(settings.dataset_base_dir / "lineitem")


@on_second_call
def get_orders_ds() -> PandasDF:
    return _read_ds(settings.dataset_base_dir / "orders")


@on_second_call
def get_customer_ds() -> PandasDF:
    return _read_ds(settings.dataset_base_dir / "customer")


@on_second_call
def get_region_ds() -> PandasDF:
    return _read_ds(settings.dataset_base_dir / "region")


@on_second_call
def get_nation_ds() -> PandasDF:
    return _read_ds(settings.dataset_base_dir / "nation")


@on_second_call
def get_supplier_ds() -> PandasDF:
    return _read_ds(settings.dataset_base_dir / "supplier")


@on_second_call
def get_part_ds() -> PandasDF:
    return _read_ds(settings.dataset_base_dir / "part")


@on_second_call
def get_part_supp_ds() -> PandasDF:
    return _read_ds(settings.dataset_base_dir / "partsupp")


def run_query(q_num: int, query: Callable[..., Any]) -> None:
    @linetimer(name=f"Overall execution of pandas Query {q_num}", unit="s")  # type: ignore[misc]
    def run() -> None:
        with CodeTimer(name=f"Get result of pandas Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            result = query()
            secs = timeit.default_timer() - t0

        if settings.run.log_timings:
            log_query_timing(
                solution="pandas",
                version=pd.__version__,
                query_number=q_num,
                time=secs,
            )

        if settings.scale_factor == 1:
            test_results(q_num, result)

        if settings.run.show_results:
            print(result)

    run()
