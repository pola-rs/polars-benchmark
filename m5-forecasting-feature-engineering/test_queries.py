from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
from duckdb_queries import q1_duckdb, q2_duckdb, q3_duckdb
from pandas_queries import q1_pandas, q2_pandas, q3_pandas
from polars_queries import q1_polars, q2_polars, q3_polars

PATH = Path("data") / "grid_part_1_small.parquet"


def test_q1():
    result_pandas = (
        q1_pandas(pd.read_parquet(PATH, engine="pyarrow"))
        .sort_values(["id", "d"])
        .reset_index(drop=True)
    )
    result_polars = (
        q1_polars(pl.scan_parquet(PATH)).collect().sort("id", "d").to_pandas()
    )
    pd.testing.assert_frame_equal(result_pandas, result_polars)

    result_duckdb = (
        q1_duckdb(duckdb.read_parquet(str(PATH)))
        .df()
        .sort_values(["id", "d"])
        .reset_index(drop=True)
    )
    # duckdb doesn't have categorical dtype
    expected = result_pandas.astype(
        {
            "id": "string[pyarrow_numpy]",
            "item_id": "string[pyarrow_numpy]",
            "dept_id": "string[pyarrow_numpy]",
            "cat_id": "string[pyarrow_numpy]",
            "store_id": "string[pyarrow_numpy]",
            "state_id": "string[pyarrow_numpy]",
        }
    )
    pd.testing.assert_frame_equal(expected, result_duckdb)


def test_q2():
    result_pandas = (
        q2_pandas(pd.read_parquet(PATH, engine="pyarrow"))
        .sort_values(["id", "d"])
        .reset_index(drop=True)
    )
    result_polars = (
        q2_polars(pl.scan_parquet(PATH)).collect().sort("id", "d").to_pandas()
    )
    pd.testing.assert_frame_equal(result_pandas, result_polars)

    result_duckdb = (
        q2_duckdb(duckdb.read_parquet(str(PATH)))
        .df()
        .sort_values(["id", "d"])
        .reset_index(drop=True)
    )
    # duckdb doesn't have categorical dtype
    expected = result_pandas.astype(
        {
            "id": "string[pyarrow_numpy]",
            "item_id": "string[pyarrow_numpy]",
            "dept_id": "string[pyarrow_numpy]",
            "cat_id": "string[pyarrow_numpy]",
            "store_id": "string[pyarrow_numpy]",
            "state_id": "string[pyarrow_numpy]",
        }
    )
    pd.testing.assert_frame_equal(expected, result_duckdb)


def test_q3():
    result_pandas = (
        q3_pandas(pd.read_parquet(PATH, engine="pyarrow"))
        .sort_values(["id", "d"])
        .reset_index(drop=True)
    )
    result_polars = (
        q3_polars(pl.scan_parquet(PATH)).collect().sort("id", "d").to_pandas()
    )
    pd.testing.assert_frame_equal(result_pandas, result_polars)

    result_duckdb = (
        q3_duckdb(duckdb.read_parquet(str(PATH)))
        .df()
        .sort_values(["id", "d"])
        .reset_index(drop=True)
    )
    # duckdb doesn't have categorical dtype
    expected = result_pandas.astype(
        {
            "id": "string[pyarrow_numpy]",
            "item_id": "string[pyarrow_numpy]",
            "dept_id": "string[pyarrow_numpy]",
            "cat_id": "string[pyarrow_numpy]",
            "store_id": "string[pyarrow_numpy]",
            "state_id": "string[pyarrow_numpy]",
        }
    )
    pd.testing.assert_frame_equal(expected, result_duckdb)
