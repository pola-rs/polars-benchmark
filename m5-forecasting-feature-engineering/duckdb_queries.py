import time
from pathlib import Path

import duckdb

print("duckdb version", duckdb.__version__)

PROCESSED_DATA_DIR = "data"

TARGET = "sales"
SHIFT_DAY = 28


# Set this to True if you just want to test that everything runs
SMALL = False
if SMALL:
    PATH = Path(PROCESSED_DATA_DIR) / "grid_part_1_small.parquet"
else:
    PATH = Path(PROCESSED_DATA_DIR) / "grid_part_1.parquet"

LAG_DAYS = list(range(SHIFT_DAY, SHIFT_DAY + 15))

WINDOW = {"partition_by": "id", "order_by": "d", "mapping_strategy": "explode"}


def q1_duckdb(df):
    return duckdb.sql("""
        SELECT
            *,
            LAG(sales, 28) OVER w AS sales_lag_28,
            LAG(sales, 29) OVER w AS sales_lag_29,
            LAG(sales, 30) OVER w AS sales_lag_30,
            LAG(sales, 31) OVER w AS sales_lag_31,
            LAG(sales, 32) OVER w AS sales_lag_32,
            LAG(sales, 33) OVER w AS sales_lag_33,
            LAG(sales, 34) OVER w AS sales_lag_34,
            LAG(sales, 35) OVER w AS sales_lag_35,
            LAG(sales, 36) OVER w AS sales_lag_36,
            LAG(sales, 37) OVER w AS sales_lag_37,
            LAG(sales, 38) OVER w AS sales_lag_38,
            LAG(sales, 39) OVER w AS sales_lag_39,
            LAG(sales, 40) OVER w AS sales_lag_40,
            LAG(sales, 41) OVER w AS sales_lag_41,
            LAG(sales, 42) OVER w AS sales_lag_42
        FROM
            df
        WINDOW w AS (PARTITION BY id ORDER BY d);""")


def q2_duckdb(df):
    return duckdb.sql("""
        WITH lagged AS (
            SELECT
                *,
                LAG(sales, 28) OVER id_w AS sales_lagged
            FROM df
            WINDOW id_w AS (PARTITION BY id ORDER BY d)
        )

        SELECT
            * EXCLUDE sales_lagged,

            -- Rolling means
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_7 = 7,
                AVG(sales_lagged) OVER roll_7, NULL) AS rolling_mean_7,
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_14 = 14,
                AVG(sales_lagged) OVER roll_14, NULL) AS rolling_mean_14,
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_30 = 30,
                AVG(sales_lagged) OVER roll_30, NULL) AS rolling_mean_30,
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_60 = 60,
                AVG(sales_lagged) OVER roll_60, NULL) AS rolling_mean_60,
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_180 = 180,
                AVG(sales_lagged) OVER roll_180, NULL) AS rolling_mean_180,

            -- Rolling standard deviations
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_7 = 7,
                STDDEV_SAMP(sales_lagged) OVER roll_7, NULL) AS rolling_std_7,
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_14 = 14,
                STDDEV_SAMP(sales_lagged) OVER roll_14, NULL) AS rolling_std_14,
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_30 = 30,
                STDDEV_SAMP(sales_lagged) OVER roll_30, NULL) AS rolling_std_30,
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_60 = 60,
                STDDEV_SAMP(sales_lagged) OVER roll_60, NULL) AS rolling_std_60,
            IF(SUM(IF(sales_lagged IS NULL, 0, 1)) OVER roll_180 = 180,
                STDDEV_SAMP(sales_lagged) OVER roll_180, NULL) AS rolling_std_180

        FROM lagged

        WINDOW
            roll_7 AS (PARTITION BY id ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
            roll_14 AS (PARTITION BY id ORDER BY d ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
            roll_30 AS (PARTITION BY id ORDER BY d ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
            roll_60 AS (PARTITION BY id ORDER BY d ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
            roll_180 AS (PARTITION BY id ORDER BY d ROWS BETWEEN 179 PRECEDING AND CURRENT ROW);
    """)


def q3_duckdb(df):
    return duckdb.sql("""
        WITH lagged AS (
            -- Create a lagged column for 'sales' with a lag of 28 days
            SELECT
                *,
                LAG(sales, 1) OVER id_w AS sales_1,
                LAG(sales, 7) OVER id_w AS sales_7,
                LAG(sales, 14) OVER id_w AS sales_14
            FROM df
            WINDOW id_w AS (PARTITION BY id ORDER BY d)
        )

        SELECT
        * EXCLUDE (sales_1, sales_7, sales_14),

        IF(SUM(IF(sales_1 IS NULL, 0, 1)) OVER roll_7 = 7,
            AVG(sales_1) OVER roll_7, NULL) AS rolling_mean_1_7,
        IF(SUM(IF(sales_1 IS NULL, 0, 1)) OVER roll_14 = 14,
            AVG(sales_1) OVER roll_14, NULL) AS rolling_mean_1_14,
        IF(SUM(IF(sales_1 IS NULL, 0, 1)) OVER roll_30 = 30,
            AVG(sales_1) OVER roll_30, NULL) AS rolling_mean_1_30,
        IF(SUM(IF(sales_1 IS NULL, 0, 1)) OVER roll_60 = 60,
            AVG(sales_1) OVER roll_60, NULL) AS rolling_mean_1_60,
        IF(SUM(IF(sales_7 IS NULL, 0, 1)) OVER roll_7 = 7,
            AVG(sales_7) OVER roll_7, NULL) AS rolling_mean_7_7,
        IF(SUM(IF(sales_7 IS NULL, 0, 1)) OVER roll_14 = 14,
            AVG(sales_7) OVER roll_14, NULL) AS rolling_mean_7_14,
        IF(SUM(IF(sales_7 IS NULL, 0, 1)) OVER roll_30 = 30,
            AVG(sales_7) OVER roll_30, NULL) AS rolling_mean_7_30,
        IF(SUM(IF(sales_7 IS NULL, 0, 1)) OVER roll_60 = 60,
            AVG(sales_7) OVER roll_60, NULL) AS rolling_mean_7_60,
        IF(SUM(IF(sales_14 IS NULL, 0, 1)) OVER roll_7 = 7,
            AVG(sales_14) OVER roll_7, NULL) AS rolling_mean_14_7,
        IF(SUM(IF(sales_14 IS NULL, 0, 1)) OVER roll_14 = 14,
            AVG(sales_14) OVER roll_14, NULL) AS rolling_mean_14_14,
        IF(SUM(IF(sales_14 IS NULL, 0, 1)) OVER roll_30 = 30,
            AVG(sales_14) OVER roll_30, NULL) AS rolling_mean_14_30,
        IF(SUM(IF(sales_14 IS NULL, 0, 1)) OVER roll_60 = 60,
            AVG(sales_14) OVER roll_60, NULL) AS rolling_mean_14_60

        FROM lagged

        -- Define rolling windows for calculating the rolling mean and standard deviation
        WINDOW
            roll_7 AS (PARTITION BY id ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
            roll_14 AS (PARTITION BY id ORDER BY d ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
            roll_30 AS (PARTITION BY id ORDER BY d ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
            roll_60 AS (PARTITION BY id ORDER BY d ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    """)


print("*** duckdb ***")

start_time = time.perf_counter()
q1_duckdb(duckdb.read_parquet(str(PATH))).to_arrow_table()
print(f"q1 took: {time.perf_counter() - start_time}")

start_time = time.perf_counter()
q2_duckdb(duckdb.read_parquet(str(PATH))).to_arrow_table()
print(f"q1 took: {time.perf_counter() - start_time}")

start_time = time.perf_counter()
q3_duckdb(duckdb.read_parquet(str(PATH))).to_arrow_table()
print(f"q1 took: {time.perf_counter() - start_time}")
