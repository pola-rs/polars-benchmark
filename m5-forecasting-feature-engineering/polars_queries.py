import os
import time

import polars as pl

print("polars version", pl.__version__)

PROCESSED_DATA_DIR = "data"

TARGET = "sales"
SHIFT_DAY = 28

# Set this to True if you just want to test that everything runs
SMALL = True
if SMALL:
    PATH = os.path.join(PROCESSED_DATA_DIR, "grid_part_1_small.parquet")
else:
    PATH = os.path.join(PROCESSED_DATA_DIR, "grid_part_1.parquet")

LAG_DAYS = [col for col in range(SHIFT_DAY, SHIFT_DAY + 15)]


def q1_polars(df):
    return df.with_columns(
        pl.col(TARGET).shift(l).over("id").alias(f"{TARGET}_lag_{l}") for l in LAG_DAYS
    )


def q2_polars(df):
    return df.with_columns(
        *[
            pl.col(TARGET)
            .shift(SHIFT_DAY)
            .rolling_mean(window_size=i)
            .over("id")
            .alias(f"rolling_mean_{i}")
            for i in [7, 14, 30, 60, 180]
        ],
        *[
            pl.col(TARGET)
            .shift(SHIFT_DAY)
            .rolling_std(window_size=i)
            .over("id")
            .alias(f"rolling_std_{i}")
            for i in [7, 14, 30, 60, 180]
        ],
    )


def q3_polars(df):
    return df.with_columns(
        pl.col(TARGET)
        .shift(d_shift)
        .rolling_mean(window_size=d_window)
        .over("id")
        .alias(f"rolling_mean_{d_shift}_{d_window}")
        for d_shift in [1, 7, 14]
        for d_window in [7, 14, 30, 60]
    )


print("*** polars lazy ***")

start_time = time.perf_counter()
q1_polars(pl.scan_parquet(PATH)).collect()
print(f"q1 took: {time.perf_counter() - start_time}")

start_time = time.perf_counter()
q2_polars(pl.scan_parquet(PATH)).collect()
print(f"q2 took: {time.perf_counter() - start_time}")

start_time = time.perf_counter()
q3_polars(pl.scan_parquet(PATH)).collect()
print(f"q2 took: {time.perf_counter() - start_time}")

print("*** polars eager ***")

start_time = time.perf_counter()
q1_polars(pl.read_parquet(PATH))
print(f"q1 took: {time.perf_counter() - start_time}")

start_time = time.perf_counter()
q2_polars(pl.read_parquet(PATH))
print(f"q2 took: {time.perf_counter() - start_time}")

start_time = time.perf_counter()
q3_polars(pl.read_parquet(PATH))
print(f"q2 took: {time.perf_counter() - start_time}")
