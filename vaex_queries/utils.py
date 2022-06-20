import timeit
from os.path import join
from typing import Callable

import vaex
from linetimer import CodeTimer, linetimer
from vaex.dataframe import DataFrame

from common_utils import (
    ANSWERS_BASE_DIR,
    DATASET_BASE_DIR,
    FILE_TYPE,
    LOG_TIMINGS,
    SHOW_RESULTS,
    append_row,
    on_second_call,
)


def _read_ds(path: str) -> DataFrame:
    path = f"{path}.{FILE_TYPE}"
    return vaex.open(path)


def get_query_answer(query: int, base_dir: str = ANSWERS_BASE_DIR) -> DataFrame:
    import pandas as pd

    answer_df = pd.read_csv(
        join(base_dir, f"q{query}.out"),
        sep="|",
        parse_dates=True,
        infer_datetime_format=True,
    )
    return answer_df.rename(columns=lambda x: x.strip())


def test_results(q_num: int, result_df: DataFrame):
    with CodeTimer(name=f"Testing result of modin Query {q_num}", unit="s"):
        result_df = result_df.to_pandas_df()
        import pandas as pd

        answer = get_query_answer(q_num)

        for c, t in answer.dtypes.items():
            s1 = result_df[c]
            s2 = answer[c]

            if t.name == "object":
                s1 = s1.astype("string").apply(lambda x: x.strip())
                s2 = s2.astype("string").apply(lambda x: x.strip())

            pd.testing.assert_series_equal(left=s1, right=s2, check_index=False)


@on_second_call
def get_line_item_ds(base_dir: str = DATASET_BASE_DIR) -> DataFrame:
    return _read_ds(join(base_dir, "lineitem"))


@on_second_call
def get_orders_ds(base_dir: str = DATASET_BASE_DIR) -> DataFrame:
    return _read_ds(join(base_dir, "orders"))


@on_second_call
def get_customer_ds(base_dir: str = DATASET_BASE_DIR) -> DataFrame:
    return _read_ds(join(base_dir, "customer"))


@on_second_call
def get_region_ds(base_dir: str = DATASET_BASE_DIR) -> DataFrame:
    return _read_ds(join(base_dir, "region"))


@on_second_call
def get_nation_ds(base_dir: str = DATASET_BASE_DIR) -> DataFrame:
    return _read_ds(join(base_dir, "nation"))


@on_second_call
def get_supplier_ds(base_dir: str = DATASET_BASE_DIR) -> DataFrame:
    return _read_ds(join(base_dir, "supplier"))


@on_second_call
def get_part_ds(base_dir: str = DATASET_BASE_DIR) -> DataFrame:
    return _read_ds(join(base_dir, "part"))


@on_second_call
def get_part_supp_ds(base_dir: str = DATASET_BASE_DIR) -> DataFrame:
    return _read_ds(join(base_dir, "partsupp"))


def run_query(q_num: int, query: Callable):
    @linetimer(name=f"Overall execution of vaex Query {q_num}", unit="s")
    def run():
        with CodeTimer(name=f"Get result of vaex Query {q_num}", unit="s"):
            t0 = timeit.default_timer()

            if LOG_TIMINGS:
                try:
                    result = query()
                    success = True
                except Exception as e:
                    print(f"vaex failed: {e}")
                    success = False
            else:
                result = query()
                success = True

            secs = timeit.default_timer() - t0

        if LOG_TIMINGS:
            append_row(
                solution=f"vaex_{FILE_TYPE}",
                version=vaex.__version__["vaex"],
                q=f"q{q_num}",
                secs=secs,
                success=success,
            )
        elif success:
            test_results(q_num, result)

        if SHOW_RESULTS:
            print(result)

    run()
