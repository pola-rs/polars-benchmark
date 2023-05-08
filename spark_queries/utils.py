import timeit
from os.path import join

from linetimer import CodeTimer, linetimer
from pandas.core.frame import DataFrame as PandasDF
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession

from common_utils import (
    ANSWERS_BASE_DIR,
    DATASET_BASE_DIR,
    LOG_TIMINGS,
    SHOW_RESULTS,
    SPARK_LOG_LEVEL,
    append_row,
    on_second_call,
)

print("SPARK_LOG_LEVEL:", SPARK_LOG_LEVEL)


def get_or_create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("spark_queries").master("local[*]").getOrCreate()
    )
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

    return spark


def __read_parquet_ds(path: str, table_name: str) -> SparkDF:
    df = get_or_create_spark().read.parquet(path)
    df.createOrReplaceTempView(table_name)
    return df


def get_query_answer(query: int, base_dir: str = ANSWERS_BASE_DIR) -> PandasDF:
    import pandas as pd

    answer_df = pd.read_csv(
        join(base_dir, f"q{query}.out"),
        sep="|",
        parse_dates=True,
    )
    return answer_df.rename(columns=lambda x: x.strip())


def test_results(q_num: int, result_df: PandasDF):
    import pandas as pd

    with CodeTimer(name=f"Testing result of Spark Query {q_num}", unit="s"):
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
def get_line_item_ds(base_dir: str = DATASET_BASE_DIR) -> SparkDF:
    return __read_parquet_ds(join(base_dir, "lineitem.parquet"), "lineitem")


@on_second_call
def get_orders_ds(base_dir: str = DATASET_BASE_DIR) -> SparkDF:
    return __read_parquet_ds(join(base_dir, "orders.parquet"), "orders")


@on_second_call
def get_customer_ds(base_dir: str = DATASET_BASE_DIR) -> SparkDF:
    return __read_parquet_ds(join(base_dir, "customer.parquet"), "customer")


@on_second_call
def get_region_ds(base_dir: str = DATASET_BASE_DIR) -> SparkDF:
    return __read_parquet_ds(join(base_dir, "region.parquet"), "region")


@on_second_call
def get_nation_ds(base_dir: str = DATASET_BASE_DIR) -> SparkDF:
    return __read_parquet_ds(join(base_dir, "nation.parquet"), "nation")


@on_second_call
def get_supplier_ds(base_dir: str = DATASET_BASE_DIR) -> SparkDF:
    return __read_parquet_ds(join(base_dir, "supplier.parquet"), "supplier")


@on_second_call
def get_part_ds(base_dir: str = DATASET_BASE_DIR) -> SparkDF:
    return __read_parquet_ds(join(base_dir, "part.parquet"), "part")


@on_second_call
def get_part_supp_ds(base_dir: str = DATASET_BASE_DIR) -> SparkDF:
    return __read_parquet_ds(join(base_dir, "partsupp.parquet"), "partsupp")


def drop_temp_view():
    spark = get_or_create_spark()
    [
        spark.catalog.dropTempView(t.name)
        for t in spark.catalog.listTables()
        if t.isTemporary
    ]


def run_query(q_num: int, result: SparkDF):
    @linetimer(name=f"Overall execution of Spark Query {q_num}", unit="s")
    def run():
        with CodeTimer(name=f"Get result of Spark Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            pdf = result.toPandas()
            secs = timeit.default_timer() - t0

        if LOG_TIMINGS:
            append_row(
                solution="spark",
                version=get_or_create_spark().version,
                q=f"q{q_num}",
                secs=secs,
            )
        else:
            test_results(q_num, pdf)

        if SHOW_RESULTS:
            print(pdf)

    run()
