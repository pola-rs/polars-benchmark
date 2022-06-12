import os
from typing import Union, final

import pandas as pd
import polars as pl
from linetimer import CodeTimer
from pandas.core.frame import DataFrame as PandasDF
from polars.internals.frame import LazyFrame as PolarsLazyDF

from polars_tpch_benchmarks import datasets


class BenchmarkQuery:
    _show_polars_plan = os.environ.get("SHOW_POLARS_PLAN", False)

    def __init__(self, query_num: int):
        self.query_num = query_num

    def get_query_result(self) -> Union[PolarsLazyDF, PandasDF]:
        """
        Implementations will override this method to add logic for query execution.
        :return: Returns one of the following - Polars lazy dataframe or Pandas dataframe
        """
        pass

    def _execute_polars_query(self, query_result: PolarsLazyDF):
        with CodeTimer(
                name=f"Overall execution of polars query {self.query_num}", unit="s"
        ):
            with CodeTimer(
                    name=f"Get result for polars query {self.query_num}", unit="s"
            ):
                final_query_result: pl.DataFrame = query_result.collect()
                print(query_result)

            with CodeTimer(
                    name=f"Get answer for polars query {self.query_num}", unit="s"
            ):
                query_answer: pl.DataFrame = datasets.get_polars_query_answer(
                    self.query_num
                ).collect()

            with CodeTimer(
                    name=f"Testing result of polars query {self.query_num}", unit="s"
            ):
                pl.testing.assert_frame_equal(
                    left=final_query_result, right=query_answer, check_dtype=False
                )

    @final
    def execute_polars_query(self):
        """
        Handles end to end execution of a polars benchmark query. This involves,
        - getting the results,
        - testing against the official TPC-H answers,
        - Logging of results and timings.
        """
        with CodeTimer(
                name=f"Overall execution of polars query {self.query_num}", unit="s"
        ):
            with CodeTimer(
                    name=f"Get result for polars query {self.query_num}", unit="s"
            ):
                query_result_ldf: PolarsLazyDF = self.get_query_result()
                if self._show_polars_plan:
                    print(query_result_ldf.describe_optimized_plan())

                query_result: pl.DataFrame = query_result_ldf.collect()
                print(query_result)

            with CodeTimer(
                    name=f"Get answer for polars query {self.query_num}", unit="s"
            ):
                query_answer: pl.DataFrame = datasets.get_polars_query_answer(
                    self.query_num
                ).collect()

            with CodeTimer(
                    name=f"Testing result of polars query {self.query_num}", unit="s"
            ):
                pl.testing.assert_frame_equal(
                    left=query_result, right=query_answer, check_dtype=False
                )

    @final
    def execute_pandas_query(self):
        """
        Handles end to end execution of a pandas benchmark query. This involves,
        - getting the results,
        - testing against the official TPC-H answers,
        - Logging of results and timings.
        """
        with CodeTimer(
                name=f"Overall execution of pandas query {self.query_num}", unit="s"
        ):
            with CodeTimer(
                    name=f"Get result for pandas query {self.query_num}", unit="s"
            ):
                query_result: PandasDF = self.get_query_result()
                print(query_result)

            with CodeTimer(
                    name=f"Get answer for pandas query {self.query_num}", unit="s"
            ):
                query_answer: PandasDF = datasets.get_pandas_query_answer(
                    self.query_num
                )

            with CodeTimer(
                    name=f"Test result of pandas query {self.query_num}", unit="s"
            ):
                pd.testing.assert_frame_equal(
                    left=query_result, right=query_answer, check_dtype=False
                )
