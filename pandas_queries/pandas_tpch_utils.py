from os.path import join

import pandas as pd
from linetimer import CodeTimer
from pandas.core.frame import DataFrame as PandasDF

__default_dataset_base_dir = "tables_scale_1"
__default_answers_base_dir = "tpch-dbgen/answers"


def __scan_parquet_ds(path: str) -> PandasDF:
    return pd.read_parquet(path)


def get_query_answer(
    query: int, base_dir: str = __default_answers_base_dir
) -> PandasDF:
    answer_df = pd.read_csv(
        join(base_dir, f"q{query}.out"),
        sep="|",
        parse_dates=True,
        infer_datetime_format=True,
    )
    return answer_df.rename(columns=lambda x: x.strip())


def test_results(q_num: int, result_df: PandasDF):
    with CodeTimer(name=f"Testing result of Query {q_num}", unit="s"):
        answer = get_query_answer(q_num)

        for c, t in answer.dtypes.items():
            s1 = result_df[c]
            s2 = answer[c]

            if t.name == "object":
                s1 = s1.astype("string").apply(lambda x: x.strip())
                s2 = s2.astype("string").apply(lambda x: x.strip())

            pd.testing.assert_series_equal(left=s1, right=s2, check_index=False)


def get_line_item_ds(base_dir: str = __default_dataset_base_dir) -> PandasDF:
    return __scan_parquet_ds(join(base_dir, "lineitem.parquet"))


def get_orders_ds(base_dir: str = __default_dataset_base_dir) -> PandasDF:
    return __scan_parquet_ds(join(base_dir, "orders.parquet"))


def get_customer_ds(base_dir: str = __default_dataset_base_dir) -> PandasDF:
    return __scan_parquet_ds(join(base_dir, "customer.parquet"))


def get_region_ds(base_dir: str = __default_dataset_base_dir) -> PandasDF:
    return __scan_parquet_ds(join(base_dir, "region.parquet"))


def get_nation_ds(base_dir: str = __default_dataset_base_dir) -> PandasDF:
    return __scan_parquet_ds(join(base_dir, "nation.parquet"))


def get_supplier_ds(base_dir: str = __default_dataset_base_dir) -> PandasDF:
    return __scan_parquet_ds(join(base_dir, "supplier.parquet"))


def get_part_ds(base_dir: str = __default_dataset_base_dir) -> PandasDF:
    return __scan_parquet_ds(join(base_dir, "part.parquet"))


def get_part_supp_ds(base_dir: str = __default_dataset_base_dir) -> PandasDF:
    return __scan_parquet_ds(join(base_dir, "partsupp.parquet"))
