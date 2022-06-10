from os.path import join

import polars as pl
from linetimer import CodeTimer

__default_dataset_base_dir = "tables_scale_1"
__default_answers_base_dir = "tpch-dbgen/answers"


def __scan_parquet_ds(path: str):
    return pl.scan_parquet(path)


def get_query_answer(query: int, base_dir: str = __default_answers_base_dir) -> pl.LazyFrame:
    answer_ldf = pl.scan_csv(join(base_dir, f"q{query}.out"), sep="|", has_header=True)
    return answer_ldf.select([pl.col(c).alias(c.strip()) for c in answer_ldf.columns])


def test_results(q_num: int, result_df: pl.DataFrame):
    with CodeTimer(name=f"Testing result of Query {q_num}", unit='s'):
        answer = get_query_answer(q_num).collect()
        pl.testing.assert_frame_equal(result_df, right=answer)


def get_line_item_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "lineitem.parquet"))


def get_orders_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "orders.parquet"))


def get_customer_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "customer.parquet"))
