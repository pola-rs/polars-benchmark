from __future__ import annotations

import timeit
from typing import TYPE_CHECKING

import pandas as pd
from linetimer import CodeTimer, linetimer
from pandas.api.types import is_string_dtype
from pandas.testing import assert_series_equal
from pyspark.sql import SparkSession

from queries.common_utils import log_query_timing, on_second_call
from settings import Settings

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame as SparkDF

settings = Settings()


def get_or_create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("spark_queries").master("local[*]").getOrCreate()
    )
    spark.sparkContext.setLogLevel(settings.run.spark_log_level)

    return spark


def _read_parquet_ds(path: Path, table_name: str) -> SparkDF:
    df = get_or_create_spark().read.parquet(str(path))
    df.createOrReplaceTempView(table_name)
    return df


@on_second_call
def get_line_item_ds() -> SparkDF:
    return _read_parquet_ds(settings.dataset_base_dir / "lineitem.parquet", "lineitem")


@on_second_call
def get_orders_ds() -> SparkDF:
    return _read_parquet_ds(settings.dataset_base_dir / "orders.parquet", "orders")


@on_second_call
def get_customer_ds() -> SparkDF:
    return _read_parquet_ds(settings.dataset_base_dir / "customer.parquet", "customer")


@on_second_call
def get_region_ds() -> SparkDF:
    return _read_parquet_ds(settings.dataset_base_dir / "region.parquet", "region")


@on_second_call
def get_nation_ds() -> SparkDF:
    return _read_parquet_ds(settings.dataset_base_dir / "nation.parquet", "nation")


@on_second_call
def get_supplier_ds() -> SparkDF:
    return _read_parquet_ds(settings.dataset_base_dir / "supplier.parquet", "supplier")


@on_second_call
def get_part_ds() -> SparkDF:
    return _read_parquet_ds(settings.dataset_base_dir / "part.parquet", "part")


@on_second_call
def get_part_supp_ds() -> SparkDF:
    return _read_parquet_ds(settings.dataset_base_dir / "partsupp.parquet", "partsupp")


def drop_temp_view() -> None:
    spark = get_or_create_spark()
    [
        spark.catalog.dropTempView(t.name)
        for t in spark.catalog.listTables()
        if t.isTemporary
    ]


def run_query(q_num: int, result: SparkDF) -> None:
    @linetimer(name=f"Overall execution of PySpark Query {q_num}", unit="s")  # type: ignore[misc]
    def run() -> None:
        with CodeTimer(name=f"Get result of PySpark Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            result_pd = result.toPandas()
            secs = timeit.default_timer() - t0

        if settings.run.log_timings:
            log_query_timing(
                solution="pyspark",
                version=get_or_create_spark().version,
                query_number=q_num,
                time=secs,
            )

        if settings.run.check_results:
            if settings.scale_factor != 1:
                msg = f"cannot check results when scale factor is not 1, got {settings.scale_factor}"
                raise RuntimeError(msg)
            _check_result(result_pd, q_num)

        if settings.run.show_results:
            print(result_pd)

    run()


def _check_result(result: pd.DataFrame, query_number: int) -> None:
    """Assert that the result of the query is correct."""
    expected = _get_query_answer(query_number)

    for c, t in expected.dtypes.items():
        s1 = result[c]
        s2 = expected[c]

        if is_string_dtype(t):
            s1 = s1.apply(lambda x: x.strip())

        assert_series_equal(left=s1, right=s2, check_index=False, check_dtype=False)


def _get_query_answer(query: int) -> pd.DataFrame:
    """Read the true answer to the query from disk."""
    path = settings.paths.answers / f"q{query}.parquet"
    return pd.read_parquet(path, dtype_backend="pyarrow")
