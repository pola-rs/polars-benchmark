from importlib.metadata import version
from pathlib import Path

import duckdb
from duckdb import DuckDBPyRelation
from linetimer import CodeTimer

from queries.common_utils import check_query_result_pl, log_query_timing
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


def run_query(query_number: int, context: DuckDBPyRelation) -> None:
    with CodeTimer(name=f"Run DuckDB query {query_number}", unit="s") as timer:
        result = context.pl()  # Force DuckDB to materialize

    if settings.run.log_timings:
        log_query_timing(
            solution="duckdb",
            version=version("duckdb"),
            query_number=query_number,
            time=timer.took,
        )

    if settings.run.check_results:
        if settings.scale_factor != 1:
            msg = f"cannot check results when scale factor is not 1, got {settings.scale_factor}"
            raise RuntimeError(msg)
        check_query_result_pl(result, query_number)

    if settings.run.show_results:
        print(result)
