import timeit
from pathlib import Path

import polars as pl
from linetimer import CodeTimer, linetimer

from queries.common_utils import check_query_result_pl, log_query_timing
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


def run_query(q_num: int, lf: pl.LazyFrame) -> None:
    @linetimer(name=f"Overall execution of polars Query {q_num}", unit="s")  # type: ignore[misc]
    def query() -> None:
        if settings.run.polars_show_plan:
            print(lf.explain())

        with CodeTimer(name=f"Get result of polars Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            result = lf.collect(streaming=settings.run.polars_streaming)

            secs = timeit.default_timer() - t0

        if settings.run.log_timings:
            log_query_timing(
                solution="polars",
                version=pl.__version__,
                query_number=q_num,
                time=secs,
            )

        if settings.run.check_results:
            if settings.scale_factor != 1:
                msg = f"cannot check results when scale factor is not 1, got {settings.scale_factor}"
                raise RuntimeError(msg)
            check_query_result_pl(result, q_num)

        if settings.run.show_results:
            print(result)

    query()
