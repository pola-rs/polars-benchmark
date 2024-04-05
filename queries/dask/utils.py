from __future__ import annotations

import timeit
from typing import TYPE_CHECKING, Any

import dask.dataframe as dd
import pandas as pd
from linetimer import CodeTimer, linetimer
from pandas.testing import assert_frame_equal

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

        if settings.run.check_results:
            if settings.scale_factor != 1:
                msg = f"cannot check results when scale factor is not 1, got {settings.scale_factor}"
                raise RuntimeError(msg)
            _check_result(result, q_num)

        if settings.run.show_results:
            print(result)

    run()


def _check_result(result: pd.DataFrame, query_number: int) -> None:
    """Assert that the result of the query is correct."""
    expected = _get_query_answer(query_number)
    assert_frame_equal(
        result.reset_index(drop=True),
        expected,
        check_dtype=False,
    )


def _get_query_answer(query: int) -> pd.DataFrame:
    """Read the true answer to the query from disk."""
    path = settings.paths.answers / f"q{query}.parquet"
    return pd.read_parquet(path, dtype_backend="pyarrow")
