import duckdb
from duckdb import DuckDBPyRelation

from queries.common_utils import check_query_result_pl, run_query_generic
from settings import Settings

settings = Settings()


def _scan_ds(table_name: str) -> str:
    path = settings.dataset_base_dir / f"{table_name}.{settings.run.file_type}"
    path_str = str(path)

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
    if settings.run.file_type == "csv":
        if settings.run.include_io:
            duckdb.read_csv(path_str)
            return f"'{path_str}'"
        else:
            name = path_str.replace("/", "_").replace(".", "_").replace("-", "_")
            duckdb.sql(
                f"create temp table if not exists {name} as select * from read_csv('{path_str}');"
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


def run_query(query_number: int, context: DuckDBPyRelation) -> None:
    query = context.pl
    run_query_generic(
        query, query_number, "duckdb", query_checker=check_query_result_pl
    )
