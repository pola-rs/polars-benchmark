from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from queries.common_utils import (
    check_query_result_pd,
    on_second_call,
    run_query_generic,
)
from settings import Settings

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame as SparkDF

settings = Settings()


def get_or_create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("spark_queries")
        .master("local[*]")
        .config("spark.driver.memory", settings.run.spark_driver_memory)
        .config("spark.executor.memory", settings.run.spark_executor_memory)
        .config("spark.log.level", settings.run.spark_log_level)
        .getOrCreate()
    )
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


def run_query(query_number: int, df: SparkDF) -> None:
    query = df.toPandas
    run_query_generic(
        query, query_number, "pyspark", query_checker=check_query_result_pd
    )
