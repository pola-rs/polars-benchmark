from __future__ import annotations

import timeit
from typing import TYPE_CHECKING, Any

import dask.dataframe as dd
import pandas as pd
from linetimer import CodeTimer, linetimer
from pandas.testing import assert_series_equal

from queries.common_utils import log_query_timing, on_second_call
from settings import Settings

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from dask.dataframe.core import DataFrame

settings = Settings()


def read_ds(path: Path) -> DataFrame:
    if settings.run.file_type != "parquet":
        msg = f"unsupported file type: {settings.run.file_type!r}"
        raise ValueError(msg)

    if settings.run.include_io:
        return dd.read_parquet(path, dtype_backend="pyarrow")  # type: ignore[attr-defined,no-any-return]

    # TODO: Load into memory before returning the Dask DataFrame.
    # Code below is tripped up by date types and pyarrow backend is not yet supported
    # return dd.from_pandas(pd.read_parquet(path), npartitions=os.cpu_count())
    return dd.read_parquet(path, dtype_backend="pyarrow")  # type: ignore[attr-defined,no-any-return]


def get_query_answer(query: int) -> pd.DataFrame:
    path = settings.paths.answers / f"q{query}.parquet"
    return pd.read_parquet(path)


def test_results(q_num: int, result_df: pd.DataFrame) -> None:
    with CodeTimer(name=f"Testing result of dask Query {q_num}", unit="s"):
        answer = get_query_answer(q_num)

        for c, t in answer.dtypes.items():
            s1 = result_df[c]
            s2 = answer[c]

            if t.name == "object":
                s1 = s1.astype("string").apply(lambda x: x.strip())
                s2 = s2.astype("string").apply(lambda x: x.strip())

            assert_series_equal(left=s1, right=s2, check_index=False, check_dtype=False)


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


def run_query(q_num: int, query: Callable[..., Any]) -> None:
    @linetimer(name=f"Overall execution of dask Query {q_num}", unit="s")  # type: ignore[misc]
    def run() -> None:
        import dask

        dask.config.set(scheduler="threads")

        with CodeTimer(name=f"Get result of dask Query {q_num}", unit="s"):
            t0 = timeit.default_timer()

            result = query()
            secs = timeit.default_timer() - t0

        if settings.run.log_timings:
            log_query_timing(
                solution="dask",
                version=dask.__version__,
                query_number=q_num,
                time=secs,
            )

        if settings.scale_factor == 1:
            test_results(q_num, result)

        if settings.run.show_results:
            print(result)

    run()
