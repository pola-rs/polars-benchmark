from pathlib import Path

import duckdb
from duckdb import DuckDBPyRelation

from queries.common_utils import check_query_result_pl, run_query_generic
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
    query = context.pl
    run_query_generic(
        query, query_number, "duckdb", query_checker=check_query_result_pl
    )
