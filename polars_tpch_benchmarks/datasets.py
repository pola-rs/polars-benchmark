from os.path import join
from typing import Type, Union

import pandas as pd
import polars as pl
from pandas.core.frame import DataFrame as PandasDF
from polars.internals.frame import LazyFrame as PolarsLazyDF

__default_dataset_base_dir = "tables_scale_1"
__default_answers_base_dir = "tpch-dbgen/answers"


def _scan_polars_parquet_ds(path: str):
    return pl.scan_parquet(path)


def _scan_pandas_parquet_ds(path: str) -> PandasDF:
    return pd.read_parquet(path)


def _scan_parquet_ds(
        azz: Union[Type[PolarsLazyDF], PolarsLazyDF[PandasDF]], path: str
) -> Union[PolarsLazyDF, PandasDF]:
    if azz == PolarsLazyDF:
        return _scan_polars_parquet_ds(path)
    else:
        return _scan_pandas_parquet_ds(path)


def get_line_item_ds(
        azz: Union[Type[PolarsLazyDF], PolarsLazyDF[PandasDF]],
        base_dir: str = __default_dataset_base_dir,
) -> Union[PolarsLazyDF, PandasDF]:
    return _scan_parquet_ds(azz, join(base_dir, "lineitem.parquet"))


def get_orders_ds(
        azz: Union[Type[PolarsLazyDF], PolarsLazyDF[PandasDF]],
        base_dir: str = __default_dataset_base_dir,
) -> Union[PolarsLazyDF, PandasDF]:
    return _scan_parquet_ds(azz, join(base_dir, "orders.parquet"))


def get_customer_ds(
        azz: Union[Type[PolarsLazyDF], PolarsLazyDF[PandasDF]],
        base_dir: str = __default_dataset_base_dir,
) -> Union[PolarsLazyDF, PandasDF]:
    return _scan_parquet_ds(azz, join(base_dir, "customer.parquet"))


def get_region_ds(
        azz: Union[Type[PolarsLazyDF], PolarsLazyDF[PandasDF]],
        base_dir: str = __default_dataset_base_dir,
) -> Union[PolarsLazyDF, PandasDF]:
    return _scan_parquet_ds(azz, join(base_dir, "region.parquet"))


def get_nation_ds(
        azz: Union[Type[PolarsLazyDF], PolarsLazyDF[PandasDF]],
        base_dir: str = __default_dataset_base_dir,
) -> Union[PolarsLazyDF, PandasDF]:
    return _scan_parquet_ds(azz, join(base_dir, "nation.parquet"))


def get_supplier_ds(
        azz: Union[Type[PolarsLazyDF], PolarsLazyDF[PandasDF]],
        base_dir: str = __default_dataset_base_dir,
) -> Union[PolarsLazyDF, PandasDF]:
    return _scan_parquet_ds(azz, join(base_dir, "supplier.parquet"))


def get_part_ds(
        azz: Union[Type[PolarsLazyDF], PolarsLazyDF[PandasDF]],
        base_dir: str = __default_dataset_base_dir,
) -> Union[PolarsLazyDF, PandasDF]:
    return _scan_parquet_ds(azz, join(base_dir, "part.parquet"))


def get_part_supp_ds(
        azz: Union[Type[PolarsLazyDF], PolarsLazyDF[PandasDF]],
        base_dir: str = __default_dataset_base_dir,
) -> Union[PolarsLazyDF, PandasDF]:
    return _scan_parquet_ds(azz, join(base_dir, "partsupp.parquet"))


def get_polars_query_answer(
        query: int, base_dir: str = __default_answers_base_dir
) -> PolarsLazyDF:
    answer_ldf = pl.scan_csv(
        join(base_dir, f"q{query}.out"), sep="|", has_header=True, parse_dates=True
    )
    cols = answer_ldf.columns
    answer_ldf = answer_ldf.select(
        [pl.col(c).alias(c.strip()) for c in cols]
    ).with_columns([pl.col(pl.datatypes.Utf8).str.strip().keep_name()])

    return answer_ldf


def get_pandas_query_answer(
        query: int, base_dir: str = __default_answers_base_dir
) -> PandasDF:
    answer_df = pd.read_csv(
        join(base_dir, f"q{query}.out"),
        sep="|",
        parse_dates=True,
        infer_datetime_format=True,
    )
    return answer_df.rename(columns=lambda x: x.strip())
