import os
import timeit
from os.path import join

import polars as pl
from linetimer import CodeTimer, linetimer
from polars import testing as pl_test

from common_utils import (
    ANSWERS_BASE_DIR,
    DATASET_BASE_DIR,
    FILE_TYPE,
    INCLUDE_IO,
    LOG_TIMINGS,
    SHOW_RESULTS,
    append_row,
)

SHOW_PLAN = bool(os.environ.get("SHOW_PLAN", False))
STREAMING = bool(os.environ.get("STREAMING", False))


def _scan_ds(path: str):
    path = f"{path}.{FILE_TYPE}"
    if FILE_TYPE == "parquet":
        scan = pl.scan_parquet(path)
    elif FILE_TYPE == "feather":
        scan = pl.scan_ipc(path)
    else:
        raise ValueError(f"file type: {FILE_TYPE} not expected")
    if INCLUDE_IO:
        return scan
    return scan.collect().rechunk().lazy()


def get_query_answer(query: int, base_dir: str = ANSWERS_BASE_DIR) -> pl.LazyFrame:
    answer_ldf = pl.scan_csv(
        join(base_dir, f"q{query}.out"),
        separator="|",
        has_header=True,
        try_parse_dates=True,
    )
    cols = answer_ldf.columns
    answer_ldf = answer_ldf.select(
        [pl.col(c).alias(c.strip()) for c in cols]
    ).with_columns([pl.col(pl.datatypes.Utf8).str.strip().keep_name()])

    return answer_ldf


def test_results(q_num: int, result_df: pl.DataFrame):
    with CodeTimer(name=f"Testing result of polars Query {q_num}", unit="s"):
        answer = get_query_answer(q_num).collect()
        pl_test.assert_frame_equal(left=result_df, right=answer, check_dtype=False)


def get_line_item_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "lineitem"))


def get_orders_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "orders"))


def get_customer_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "customer"))


def get_region_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "region"))


def get_nation_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "nation"))


def get_supplier_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "supplier"))


def get_part_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "part"))


def get_part_supp_ds(base_dir: str = DATASET_BASE_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "partsupp"))


def run_query(q_num: int, lp: pl.LazyFrame):
    @linetimer(name=f"Overall execution of polars Query {q_num}", unit="s")
    def query():
        if SHOW_PLAN:
            print(lp.explain())

        with CodeTimer(name=f"Get result of polars Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            result = lp.collect(streaming=STREAMING)

            secs = timeit.default_timer() - t0

        if LOG_TIMINGS:
            append_row(
                solution="polars", version=pl.__version__, q=f"q{q_num}", secs=secs
            )
        else:
            test_results(q_num, result)

        if SHOW_RESULTS:
            print(result)

    query()
