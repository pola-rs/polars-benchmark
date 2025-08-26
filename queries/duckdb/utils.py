import duckdb
from duckdb import DuckDBPyRelation

from queries.common_utils import (
    check_query_result_pl,
    get_table_path,
    run_query_generic,
)
from settings import Settings
import os

settings = Settings()
_connection = None

def _scan_ds(table_name: str) -> str:
    path = get_table_path(table_name)
    path_str = str(path)

    if settings.run.io_type == "skip":
        return table_name
    elif settings.run.io_type == "parquet":
        return f"'{path_str}'"
    elif settings.run.io_type == "csv":
        return f"'{path_str}'"
    else:
        msg = f"unsupported file type: {settings.run.io_type!r}"
        raise ValueError(msg)


def get_line_item_ds() -> str:
    return _scan_ds("lineitem")


def get_orders_ds() -> str:
    return _scan_ds("orders")


def get_customer_ds() -> str:
    return _scan_ds("customer")


def get_region_ds() -> str:
    return _scan_ds("region")


def get_nation_ds() -> str:
    return _scan_ds("nation")


def get_supplier_ds() -> str:
    return _scan_ds("supplier")


def get_part_ds() -> str:
    return _scan_ds("part")


def get_part_supp_ds() -> str:
    return _scan_ds("partsupp")

def get_persistent_path() -> str:
    return os.path.join(os.path.dirname(get_table_path('lineitem')), 'tpch.db')

def get_connection():
    global _connection
    if _connection is None:
        if settings.run.io_type == "skip":
            # connect to persistent db
            _connection = duckdb.connect(get_persistent_path())
        else:
            # connect to in-memory db
            _connection = duckdb.connect()
    return _connection

def run_query(query_number: int, query: str) -> None:
    conn = get_connection()
    if settings.run.show_results:
        def execute() -> None:
            print(conn.sql(query))
    elif settings.run.check_results:
        def execute() -> None:
            return conn.sql(query).pl()
    else:
        def execute() -> None:
            return conn.sql(query).fetchall()

    run_query_generic(
        execute, query_number, "duckdb", query_checker=check_query_result_pl
    )
