from __future__ import annotations

from typing import TYPE_CHECKING

from linetimer import CodeTimer
from pyspark.sql import SparkSession

from queries.common_utils import check_query_result_pd, log_query_timing, on_second_call
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


def run_query(query_number: int, query: SparkDF) -> None:
    with CodeTimer(name=f"Run PySpark query {query_number}", unit="s") as timer:
        result = query.toPandas()

    if settings.run.log_timings:
        log_query_timing(
            solution="pyspark",
            version=get_or_create_spark().version,
            query_number=query_number,
            time=timer.took,
        )

    if settings.run.check_results:
        if settings.scale_factor != 1:
            msg = f"cannot check results when scale factor is not 1, got {settings.scale_factor}"
            raise RuntimeError(msg)
        check_query_result_pd(result, query_number)

    if settings.run.show_results:
        print(result)
