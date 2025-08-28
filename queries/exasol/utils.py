from __future__ import annotations

import time
from contextlib import suppress
from glob import glob
from pathlib import Path

import pandas as _pd
import pyarrow.parquet as pq
import pyexasol

from queries.common_utils import run_query_generic
from settings import Settings

settings = Settings()

_connection: pyexasol.ExaConnection | None = None


def get_connection() -> pyexasol.ExaConnection:
    global _connection
    if _connection is None:
        dsn = f"{settings.exasol.host}:{settings.exasol.port}"
        schema = settings.exasol.schema_name or settings.exasol.user
        _connection = pyexasol.connect(
            dsn=dsn,
            user=settings.exasol.user,
            password=settings.exasol.password,
            schema=schema,
        )
    return _connection


def get_line_item_ds() -> str:
    return "lineitem"


def get_orders_ds() -> str:
    return "orders"


def get_customer_ds() -> str:
    return "customer"


def get_region_ds() -> str:
    return "region"


def get_nation_ds() -> str:
    return "nation"


def get_supplier_ds() -> str:
    return "supplier"


def get_part_ds() -> str:
    return "part"


def get_part_supp_ds() -> str:
    return "partsupp"


def get_db_library_version() -> str:
    """Get the Exasol database version via SQL."""
    conn = get_connection()
    row = conn.execute(
        "SELECT PARAM_VALUE FROM SYS.EXA_METADATA "
        "WHERE PARAM_NAME='databaseProductVersion'"
    ).fetchall()[0]
    return row[0]


_TABLE_NAMES = [
    "nation",
    "region",
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
]

# Mapping of TPC-H table to its columns for CSV import (ignore trailing empty field)
_TABLE_COLUMNS: dict[str, list[str]] = {
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "region": ["r_regionkey", "r_name", "r_comment"],
    "part": [
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment",
    ],
    "supplier": [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment",
    ],
    "partsupp": [
        "ps_partkey",
        "ps_suppkey",
        "ps_availqty",
        "ps_supplycost",
        "ps_comment",
    ],
    "customer": [
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment",
    ],
    "orders": [
        "o_orderkey",
        "o_custkey",
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority",
        "o_comment",
    ],
    "lineitem": [
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "l_comment",
    ],
}


# ----------------------------------------------------------------------------
def prepare_schema() -> None:
    """Run create_schema.sql to create Exasol tables before loading data."""
    conn = get_connection()
    scripts_dir = Path(__file__).parent / "queries"
    sql = (scripts_dir / "create_schema.sql").read_text()
    for stmt in sql.split(";"):
        stmt_str = stmt.strip()
        if not stmt_str:
            continue
        if stmt_str.lower().startswith("set autocommit"):
            continue
        conn.execute(stmt_str)


# ----------------------------------------------------------------------------
def prepare_postload() -> None:
    """Run post-load SQL to index tables and gather statistics."""
    conn = get_connection()
    scripts_dir = Path(__file__).parent / "queries"
    for script in ("create_indices_1node.sql", "analyze_database.sql"):
        sql = (scripts_dir / script).read_text()
        for stmt in sql.split(";"):
            stmt_str = stmt.strip()
            if not stmt_str:
                continue
            if stmt_str.lower().startswith("set autocommit"):
                continue
            conn.execute(stmt_str)


def prepare_load_data() -> None:
    """Load TPC-H data files into Exasol tables using IMPORT FROM files.

    Verify row counts.
    """
    conn = get_connection()
    base_dir = settings.dataset_base_dir
    overall_start = time.perf_counter()
    for table in _TABLE_NAMES:
        table_start = time.perf_counter()
        # compute expected row count from parquet metadata if present
        expected_count = 0
        for pq_file in sorted(Path(base_dir).glob(f"{table}.parquet*")):
            expected_count += pq.ParquetFile(str(pq_file)).metadata.num_rows

        cols = _TABLE_COLUMNS[table]
        csv_cols = [f"1..{len(cols)}"]
        pattern = str(base_dir / f"{table}.tbl*")
        for file_path in sorted(glob(pattern)):  # noqa: PTH207
            conn.import_from_file(
                file_path,
                table=table,
                import_params={
                    "columns": cols,
                    "csv_cols": csv_cols,
                    "column_separator": "|",
                },
            )

        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchall()[0]
        count = row[0]
        if count == 0:
            msg = f"No rows loaded into Exasol table '{table}'"
            raise RuntimeError(msg)
        if expected_count and count != expected_count:
            msg = (
                f"Row count mismatch for '{table}': loaded {count:,} rows "
                f"but expected {expected_count:,}"
            )
            raise RuntimeError(msg)

        elapsed = time.perf_counter() - table_start
        if expected_count:
            print(
                f"{table:>10}: loaded {count:,}/{expected_count:,} rows in {elapsed:.2f}s",
                flush=True,
            )
        else:
            print(f"{table:>10}: loaded {count:,} rows in {elapsed:.2f}s", flush=True)

    total_elapsed = time.perf_counter() - overall_start
    print(f"Total load time: {total_elapsed:.2f}s", flush=True)


def _check_query_result_exasol(result: _pd.DataFrame, query_number: int) -> None:
    """Assert that the given pandas DataFrame matches the expected answer.

    Apply Exasol-specific normalization (case, whitespace, types, dates).
    """
    import warnings

    import pandas as pd
    from pandas.testing import assert_frame_equal

    from queries.common_utils import _get_query_answer_pd

    expected = _get_query_answer_pd(query_number)
    # detect which columns are string/extension dtype in the expected answers
    string_cols = [
        col.lower()
        for col in expected.columns
        if pd.api.types.is_string_dtype(expected[col])
        or pd.api.types.is_object_dtype(expected[col])
    ]
    # normalize column names to lowercase for comparison
    got = result.reset_index(drop=True).copy()
    got.columns = [c.lower() for c in got.columns]
    exp = expected.copy()
    exp.columns = [c.lower() for c in exp.columns]
    for col in string_cols:
        if col in got.columns and col in exp.columns:
            got[col] = got[col].astype(str).str.strip()
            exp[col] = exp[col].astype(str).str.strip()
    for col in exp.columns:
        with suppress(Exception):
            exp[col] = exp[col].to_numpy()
    for col in set(got.columns).intersection(exp.columns):
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", message="Could not infer format.*", category=UserWarning
                )
                got[col] = pd.to_datetime(got[col]).dt.strftime("%Y-%m-%d")
                exp[col] = pd.to_datetime(exp[col]).dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    assert_frame_equal(got, exp, check_dtype=False)


def run_query(query_number: int, query: str) -> None:
    conn = get_connection()

    if not (settings.run.show_results or settings.run.check_results):

        def execute() -> None:
            cursor = conn.execute(query)
            with suppress(Exception):
                cursor.fetchall()
            return None
    else:

        def execute() -> _pd.DataFrame:
            cursor = conn.execute(query)
            rows = cursor.fetchall()
            cols = cursor.column_names()
            df = _pd.DataFrame(rows, columns=cols)
            # Round DECIMAL columns to their defined scale
            for name, dtype in cursor.columns().items():
                scale = None
                if isinstance(dtype, dict):
                    if dtype.get("type", "").upper() == "DECIMAL":
                        scale = dtype.get("scale")
                if scale is not None:
                    vals = df[name].astype(float)
                    df[name] = vals.round(scale)
            return df

    run_query_generic(
        execute,
        query_number,
        "exasol",
        library_version=get_db_library_version(),
        query_checker=_check_query_result_exasol,
    )
