import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow

print("pandas version", pd.__version__)
print("numpy version", np.__version__)
print("pyarrow version", pyarrow.__version__)

pd.options.mode.copy_on_write = True
pd.options.future.infer_string = True

PROCESSED_DATA_DIR = "data"

TARGET = "sales"
SHIFT_DAY = 28

# Set this to True if you just want to test that everything runs
SMALL = True
if SMALL:
    PATH = Path(PROCESSED_DATA_DIR) / "grid_part_1_small.parquet"
else:
    PATH = Path(PROCESSED_DATA_DIR) / "grid_part_1.parquet"

LAG_DAYS = list(range(SHIFT_DAY, SHIFT_DAY + 15))


def q1_pandas(df):
    return df.assign(
        **{
            f"{TARGET}_lag_{lag}": df.groupby(["id"], observed=True)[TARGET].transform(
                lambda x: x.shift(lag)  # noqa: B023
            )
            for lag in LAG_DAYS
        }
    )


def q2_pandas(df):
    for i in [7, 14, 30, 60, 180]:
        df["rolling_mean_" + str(i)] = df.groupby(["id"], observed=True)[
            TARGET
        ].transform(lambda x: x.shift(SHIFT_DAY).rolling(i).mean())  # noqa: B023
    for i in [7, 14, 30, 60, 180]:
        df["rolling_std_" + str(i)] = df.groupby(["id"], observed=True)[
            TARGET
        ].transform(lambda x: x.shift(SHIFT_DAY).rolling(i).std())  # noqa: B023
    return df


def q3_pandas(df):
    for d_shift in [1, 7, 14]:
        for d_window in [7, 14, 30, 60]:
            col_name = "rolling_mean_" + str(d_shift) + "_" + str(d_window)
            df[col_name] = df.groupby(["id"], observed=True)[TARGET].transform(
                lambda x: x.shift(d_shift).rolling(d_window).mean()  # noqa: B023
            )
    return df


start_time = time.perf_counter()
q1_pandas(pd.read_parquet(PATH, engine="pyarrow"))
print(f"q1 took: {time.perf_counter() - start_time}")

start_time = time.perf_counter()
q2_pandas(pd.read_parquet(PATH, engine="pyarrow"))
print(f"q2 took: {time.perf_counter() - start_time}")

start_time = time.perf_counter()
q3_pandas(pd.read_parquet(PATH, engine="pyarrow"))
print(f"q2 took: {time.perf_counter() - start_time}")
