import timeit
from importlib.metadata import version
from pathlib import Path

import duckdb
import polars as pl
from duckdb import DuckDBPyRelation
from linetimer import CodeTimer, linetimer
from polars.testing import assert_frame_equal

from queries.common_utils import append_row
from settings import Settings

settings = Settings()


def _scan_ds(path: Path) -> str:
    path_str = f"{path}.{settings.run.file_type}"
    if settings.run.file_type == "parquet":
        if settings.run.include_io:
            duckdb.read_parquet(path_str)
            return f"'{path_str}'"
        else:
            name = path_str.replace("/", "_").replace(".", "_").replace("-", "_")
            duckdb.sql(
                f"create temp table if not exists {name} as select * from read_parquet('{path_str}');"
            )
            return name
    elif settings.run.file_type == "feather":
        msg = "duckdb does not support feather for now"
        raise ValueError(msg)
    else:
        msg = f"unsupported file type: {settings.run.file_type!r}"
        raise ValueError(msg)
    return path_str


def get_query_answer(query: int) -> pl.LazyFrame:
    path = settings.paths.answers / f"q{query}.parquet"
    return pl.scan_parquet(path)


def test_results(q_num: int, result_df: pl.DataFrame) -> None:
    with CodeTimer(name=f"Testing result of duckdb Query {q_num}", unit="s"):
        answer = get_query_answer(q_num).collect()
        assert_frame_equal(left=result_df, right=answer, check_dtype=False)


def get_line_item_ds() -> str:
    return _scan_ds(settings.dataset_base_dir / "lineitem")


def get_orders_ds() -> str:
    return _scan_ds(settings.dataset_base_dir / "orders")


def get_customer_ds() -> str:
    return _scan_ds(settings.dataset_base_dir / "customer")


def get_region_ds() -> str:
    return _scan_ds(settings.dataset_base_dir / "region")


def get_nation_ds() -> str:
    return _scan_ds(settings.dataset_base_dir / "nation")


def get_supplier_ds() -> str:
    return _scan_ds(settings.dataset_base_dir / "supplier")


def get_part_ds() -> str:
    return _scan_ds(settings.dataset_base_dir / "part")


def get_part_supp_ds() -> str:
    return _scan_ds(settings.dataset_base_dir / "partsupp")


def run_query(q_num: int, context: DuckDBPyRelation) -> None:
    @linetimer(name=f"Overall execution of duckdb Query {q_num}", unit="s")  # type: ignore[misc]
    def query() -> None:
        with CodeTimer(name=f"Get result of duckdb Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            # force duckdb to materialize
            result = context.pl()

            secs = timeit.default_timer() - t0

        if settings.run.log_timings:
            append_row(
                solution="duckdb", version=version("duckdb"), q=f"q{q_num}", secs=secs
            )
        else:
            test_results(q_num, result)

        if settings.run.show_results:
            print(result)

    query()
