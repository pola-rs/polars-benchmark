import timeit
from importlib.metadata import version
from pathlib import Path

import duckdb
import polars as pl
from duckdb import DuckDBPyRelation
from linetimer import CodeTimer, linetimer
from polars.testing import assert_frame_equal

from queries.common_utils import (
    ANSWERS_BASE_DIR,
    DATASET_BASE_DIR,
    FILE_TYPE,
    INCLUDE_IO,
    LOG_TIMINGS,
    SHOW_RESULTS,
    append_row,
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


def get_query_answer(query: int, base_dir: Path = ANSWERS_BASE_DIR) -> pl.LazyFrame:
    path = base_dir / f"q{query}.parquet"
    return pl.scan_parquet(path)


def test_results(q_num: int, result_df: pl.DataFrame) -> None:
    with CodeTimer(name=f"Testing result of duckdb Query {q_num}", unit="s"):
        answer = get_query_answer(q_num).collect()
        assert_frame_equal(left=result_df, right=answer, check_dtype=False)


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
    @linetimer(name=f"Overall execution of duckdb Query {q_num}", unit="s")
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

        if SHOW_RESULTS:
            print(result)

    query()
