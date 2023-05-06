import timeit
from os.path import join
from typing import Callable

import modin
import modin.pandas as pd
import pandas
from linetimer import CodeTimer, linetimer
from pandas.core.frame import DataFrame as PandasDF

from common_utils import (
    ANSWERS_BASE_DIR,
    DATASET_BASE_DIR,
    LOG_TIMINGS,
    SHOW_RESULTS,
    append_row,
    on_second_call,
)


def __read_parquet_ds(path: str) -> PandasDF:
    return pd.read_parquet(path, dtype_backend="pyarrow", engine="pyarrow")


def get_query_answer(query: int, base_dir: str = ANSWERS_BASE_DIR) -> PandasDF:
    import pandas as pd

    answer_df = pd.read_csv(
        join(base_dir, f"q{query}.out"),
        sep="|",
        parse_dates=True,
        infer_datetime_format=True,
    )
    return answer_df.rename(columns=lambda x: x.strip())


def test_results(q_num: int, result_df: PandasDF):
    with CodeTimer(name=f"Testing result of modin Query {q_num}", unit="s"):
        import pandas as pd

        answer = get_query_answer(q_num)

        for c, t in answer.dtypes.items():
            s1 = result_df[c].series()
            s2 = answer[c]

            if t.name == "object":
                s1 = s1.astype("string").apply(lambda x: x.strip())
                s2 = s2.astype("string").apply(lambda x: x.strip())

            pd.testing.assert_series_equal(left=s1, right=s2, check_index=False)


@on_second_call
def get_line_item_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return __read_parquet_ds(join(base_dir, "lineitem.parquet"))


@on_second_call
def get_orders_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return __read_parquet_ds(join(base_dir, "orders.parquet"))


@on_second_call
def get_customer_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return __read_parquet_ds(join(base_dir, "customer.parquet"))


@on_second_call
def get_region_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return __read_parquet_ds(join(base_dir, "region.parquet"))


@on_second_call
def get_nation_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return __read_parquet_ds(join(base_dir, "nation.parquet"))


@on_second_call
def get_supplier_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return __read_parquet_ds(join(base_dir, "supplier.parquet"))


@on_second_call
def get_part_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return __read_parquet_ds(join(base_dir, "part.parquet"))


@on_second_call
def get_part_supp_ds(base_dir: str = DATASET_BASE_DIR) -> PandasDF:
    return __read_parquet_ds(join(base_dir, "partsupp.parquet"))


def run_query(q_num: int, query: Callable):
    @linetimer(name=f"Overall execution of modin Query {q_num}", unit="s")
    def run():
        with CodeTimer(name=f"Get result of modin Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            result = query()
            secs = timeit.default_timer() - t0

        if LOG_TIMINGS:
            append_row(
                solution="modin", version=modin.__version__, q=f"q{q_num}", secs=secs
            )
        else:
            pass
            # need to convert to pandas first
            # need to figure out how
            # test_results(q_num, result)

        if SHOW_RESULTS:
            print(result)

    run()
