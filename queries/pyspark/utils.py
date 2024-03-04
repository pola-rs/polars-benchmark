import timeit
from pathlib import Path

from linetimer import CodeTimer, linetimer
from pandas.core.frame import DataFrame as PandasDF
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession

from queries.common_utils import (
    ANSWERS_BASE_DIR,
    DATASET_BASE_DIR,
    LOG_TIMINGS,
    SHOW_RESULTS,
    append_row,
    on_second_call,
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


def get_query_answer(query: int, base_dir: Path = ANSWERS_BASE_DIR) -> PandasDF:
    import pandas as pd

    path = base_dir / f"q{query}.parquet"
    return pd.read_parquet(path)


def test_results(q_num: int, result_df: PandasDF) -> None:
    import pandas as pd

    with CodeTimer(name=f"Testing result of PySpark Query {q_num}", unit="s"):
        answer = get_query_answer(q_num)

        for c, t in answer.dtypes.items():
            s1 = result_df[c]
            s2 = answer[c]

            if t.name == "object":
                s1 = s1.astype("string").apply(lambda x: x.strip())
                s2 = s2.astype("string").apply(lambda x: x.strip())

            elif t.name.startswith("int"):
                s1 = s1.astype("int64")
                s2 = s2.astype("int64")

            pd.testing.assert_series_equal(left=s1, right=s2, check_index=False)


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

        if SHOW_RESULTS:
            print(pdf)

    run()
