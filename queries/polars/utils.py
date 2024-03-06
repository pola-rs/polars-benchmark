import polars as pl
from linetimer import CodeTimer
from polars.testing import assert_frame_equal

from queries.common_utils import (
    DATASET_BASE_DIR,
    append_row,
    settings,
)
from queries.settings import FILE_FORMAT


def check_result(result: pl.DataFrame, query_number: int) -> None:
    """Check the result of a query against the pre-defined answers."""
    answer = _get_query_answer(query_number)
    assert_frame_equal(result, answer, check_dtype=False)


def _get_query_answer(query_number: int) -> pl.DataFrame:
    """Load the query answer from a Parquet file."""
    file_name = f"q{query_number}.parquet"
    file_path = settings.paths.answers / file_name
    return pl.read_parquet(file_path)


def _scan_table(table_name: str, file_format: FILE_FORMAT) -> pl.LazyFrame:
    preload_data = file_format == "skip"
    if preload_data:
        file_format = "parquet"

    file_name = f"{table_name}.{file_format}"
    file_path = DATASET_BASE_DIR / file_name

    if file_format == "parquet":
        scan = pl.scan_parquet(file_path)
    elif file_format == "feather":
        scan = pl.scan_ipc(file_path)
    else:
        msg = f"unexpected file format: {file_format}"
        raise ValueError(msg)

    if preload_data:
        return scan.collect().rechunk().lazy()

    return scan


def get_customer_ds(file_format: FILE_FORMAT = "skip") -> pl.LazyFrame:
    return _scan_table("customer", file_format)


def get_line_item_ds(file_format: FILE_FORMAT = "skip") -> pl.LazyFrame:
    return _scan_table("lineitem", file_format)


def get_nation_ds(file_format: FILE_FORMAT = "skip") -> pl.LazyFrame:
    return _scan_table("nation", file_format)


def get_orders_ds(file_format: FILE_FORMAT = "skip") -> pl.LazyFrame:
    return _scan_table("orders", file_format)


def get_part_ds(file_format: FILE_FORMAT = "skip") -> pl.LazyFrame:
    return _scan_table("part", file_format)


def get_part_supp_ds(file_format: FILE_FORMAT = "skip") -> pl.LazyFrame:
    return _scan_table("partsupp", file_format)


def get_region_ds(file_format: FILE_FORMAT = "skip") -> pl.LazyFrame:
    return _scan_table("region", file_format)


def get_supplier_ds(file_format: FILE_FORMAT = "skip") -> pl.LazyFrame:
    return _scan_table("supplier", file_format)


def run_query(
    query_number: int,
    query_plan: pl.LazyFrame,
    print_query_plan: bool = False,
    streaming: bool = False,
) -> None:
    if print_query_plan:
        print(query_plan.explain())

    with CodeTimer(name=f"Execute Polars query {query_number}", unit="s") as timer:
        result = query_plan.collect(streaming=streaming)

    if settings.print_query_output:
        print(result)

    check_result(result, query_number)

    if settings.paths.timings is not None:
        append_row(
            solution="polars",
            version=pl.__version__,
            query_number=query_number,
            time=timer.took,
        )
