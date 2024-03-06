import timeit
from pathlib import Path

import pandas as pd
from linetimer import CodeTimer, linetimer
from pandas.testing import assert_series_equal
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession

from queries.common_utils import (
    DATASET_BASE_DIR,
    LOG_TIMINGS,
    append_row,
    on_second_call,
    settings,
)


def get_or_create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("spark_queries").master("local[*]").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def _read_parquet_ds(path: Path, table_name: str) -> SparkDF:
    df = get_or_create_spark().read.parquet(str(path))
    df.createOrReplaceTempView(table_name)
    return df


def test_results(query_number: int, result_df: pd.DataFrame) -> None:
    answer = _get_query_answer(query_number)

    for c, t in answer.dtypes.items():
        s1 = result_df[c]
        s2 = answer[c]

        if t.name == "object":
            s1 = s1.astype("string").apply(lambda x: x.strip())
            s2 = s2.astype("string").apply(lambda x: x.strip())

        elif t.name.startswith("int"):
            s1 = s1.astype("int64")
            s2 = s2.astype("int64")

        assert_series_equal(left=s1, right=s2, check_index=False, check_dtype=False)


def _get_query_answer(query_number: int) -> pd.DataFrame:
    file_name = f"q{query_number}.parquet"
    file_path = settings.paths.answers / file_name
    return pd.read_parquet(file_path, dtype_backend="pyarrow")


@on_second_call
def get_line_item_ds(base_dir: Path = DATASET_BASE_DIR) -> SparkDF:
    return _read_parquet_ds(base_dir / "lineitem.parquet", "lineitem")


@on_second_call
def get_orders_ds(base_dir: Path = DATASET_BASE_DIR) -> SparkDF:
    return _read_parquet_ds(base_dir / "orders.parquet", "orders")


@on_second_call
def get_customer_ds(base_dir: Path = DATASET_BASE_DIR) -> SparkDF:
    return _read_parquet_ds(base_dir / "customer.parquet", "customer")


@on_second_call
def get_region_ds(base_dir: Path = DATASET_BASE_DIR) -> SparkDF:
    return _read_parquet_ds(base_dir / "region.parquet", "region")


@on_second_call
def get_nation_ds(base_dir: Path = DATASET_BASE_DIR) -> SparkDF:
    return _read_parquet_ds(base_dir / "nation.parquet", "nation")


@on_second_call
def get_supplier_ds(base_dir: Path = DATASET_BASE_DIR) -> SparkDF:
    return _read_parquet_ds(base_dir / "supplier.parquet", "supplier")


@on_second_call
def get_part_ds(base_dir: Path = DATASET_BASE_DIR) -> SparkDF:
    return _read_parquet_ds(base_dir / "part.parquet", "part")


@on_second_call
def get_part_supp_ds(base_dir: Path = DATASET_BASE_DIR) -> SparkDF:
    return _read_parquet_ds(base_dir / "partsupp.parquet", "partsupp")


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
            pdf = result.toPandas()
            secs = timeit.default_timer() - t0

        if LOG_TIMINGS:
            append_row(
                solution="pyspark",
                version=get_or_create_spark().version,
                q=f"q{q_num}",
                secs=secs,
            )
        else:
            test_results(q_num, pdf)

        if settings.print_query_output:
            print(pdf)

    run()
