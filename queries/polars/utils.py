import timeit
from pathlib import Path

import polars as pl
from linetimer import CodeTimer, linetimer
from polars.testing import assert_frame_equal

from queries.common_utils import log_query_timing
from settings import Settings

settings = Settings()


def _scan_ds(path: Path) -> pl.LazyFrame:
    path_str = f"{path}.{settings.run.file_type}"
    if settings.run.file_type == "parquet":
        scan = pl.scan_parquet(path_str)
    elif settings.run.file_type == "feather":
        scan = pl.scan_ipc(path_str)
    else:
        msg = f"unsupported file type: {settings.run.file_type!r}"
        raise ValueError(msg)
    if settings.run.include_io:
        return scan
    return scan.collect().rechunk().lazy()


def get_query_answer(query: int) -> pl.LazyFrame:
    path = settings.paths.answers / f"q{query}.parquet"
    return pl.scan_parquet(path)


def test_results(q_num: int, result_df: pl.DataFrame) -> None:
    with CodeTimer(name=f"Testing result of polars Query {q_num}", unit="s"):
        answer = get_query_answer(q_num).collect()
        assert_frame_equal(left=result_df, right=answer, check_dtype=False)


def get_line_item_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "lineitem")


def get_orders_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "orders")


def get_customer_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "customer")


def get_region_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "region")


def get_nation_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "nation")


def get_supplier_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "supplier")


def get_part_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "part")


def get_part_supp_ds() -> pl.LazyFrame:
    return _scan_ds(settings.dataset_base_dir / "partsupp")


def run_query(q_num: int, lp: pl.LazyFrame) -> None:
    @linetimer(name=f"Overall execution of polars Query {q_num}", unit="s")  # type: ignore[misc]
    def query() -> None:
        if settings.run.polars_show_plan:
            print(lp.explain())

        with CodeTimer(name=f"Get result of polars Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            result = lp.collect(streaming=settings.run.polars_streaming)

            secs = timeit.default_timer() - t0

        if settings.run.log_timings:
            log_query_timing(
                solution="polars",
                version=pl.__version__,
                query_number=q_num,
                time=secs,
            )

        if settings.scale_factor == 1:
            test_results(q_num, result)

        if settings.run.show_results:
            print(result)

    query()
