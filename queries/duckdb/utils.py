import timeit
from importlib.metadata import version
from pathlib import Path

import duckdb
import polars as pl
from duckdb import DuckDBPyRelation
from linetimer import CodeTimer, linetimer
from polars.testing import assert_frame_equal

from queries.common_utils import (
    DATASET_BASE_DIR,
    FILE_TYPE,
    INCLUDE_IO,
    LOG_TIMINGS,
    append_row,
    settings,
)


def _scan_ds(path: Path) -> str:
    path_str = f"{path}.{FILE_TYPE}"
    if FILE_TYPE == "parquet":
        if INCLUDE_IO:
            duckdb.read_parquet(path_str)
            return f"'{path_str}'"
        else:
            name = path_str.replace("/", "_").replace(".", "_").replace("-", "_")
            duckdb.sql(
                f"create temp table if not exists {name} as select * from read_parquet('{path_str}');"
            )
            return name
    elif FILE_TYPE == "feather":
        msg = "duckdb does not support feather for now"
        raise ValueError(msg)
    else:
        msg = f"file type: {FILE_TYPE} not expected"
        raise ValueError(msg)
    return path_str


def test_results(query_number: int, result: pl.DataFrame) -> None:
    answer = _get_query_answer(query_number)
    assert_frame_equal(result, answer, check_dtype=False)


def _get_query_answer(query_number: int) -> pl.DataFrame:
    file_name = f"q{query_number}.parquet"
    file_path = settings.paths.answers / file_name
    return pl.read_parquet(file_path)


def get_line_item_ds(base_dir: Path = DATASET_BASE_DIR) -> str:
    return _scan_ds(base_dir / "lineitem")


def get_orders_ds(base_dir: Path = DATASET_BASE_DIR) -> str:
    return _scan_ds(base_dir / "orders")


def get_customer_ds(base_dir: Path = DATASET_BASE_DIR) -> str:
    return _scan_ds(base_dir / "customer")


def get_region_ds(base_dir: Path = DATASET_BASE_DIR) -> str:
    return _scan_ds(base_dir / "region")


def get_nation_ds(base_dir: Path = DATASET_BASE_DIR) -> str:
    return _scan_ds(base_dir / "nation")


def get_supplier_ds(base_dir: Path = DATASET_BASE_DIR) -> str:
    return _scan_ds(base_dir / "supplier")


def get_part_ds(base_dir: Path = DATASET_BASE_DIR) -> str:
    return _scan_ds(base_dir / "part")


def get_part_supp_ds(base_dir: Path = DATASET_BASE_DIR) -> str:
    return _scan_ds(base_dir / "partsupp")


def run_query(q_num: int, context: DuckDBPyRelation) -> None:
    @linetimer(name=f"Overall execution of duckdb Query {q_num}", unit="s")  # type: ignore[misc]
    def query() -> None:
        with CodeTimer(name=f"Get result of duckdb Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            # force duckdb to materialize
            result = context.pl()

            secs = timeit.default_timer() - t0

        if LOG_TIMINGS:
            append_row(
                solution="duckdb", version=version("duckdb"), q=f"q{q_num}", secs=secs
            )
        else:
            test_results(q_num, result)

        if settings.print_query_output:
            print(result)

    query()
