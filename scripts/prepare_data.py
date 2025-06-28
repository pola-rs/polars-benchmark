"""Disclaimer.

Certain portions of the contents of this file are derived from TPC-H version 3.0.1
(retrieved from
http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
Such portions are subject to copyrights held by Transaction Processing
Performance Council (“TPC”) and licensed under the TPC EULA is available at
http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
(the “TPC EULA”).

You may not use this file except in compliance with the TPC EULA.
DISCLAIMER: Portions of this file is derived from the TPC-H benchmark and as
such any result obtained using this file are not comparable to published TPC-H
Benchmark results, as the results obtained from using this file do not comply with
the TPC-H Benchmark.
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import pathlib
import shlex
import shutil
import subprocess
from multiprocessing import Pool
from typing import no_type_check

import polars as pl

from settings import Settings

tpch_dbgen = pathlib.Path(__file__).parent.parent / "tpch-dbgen"


settings = Settings()


logger = logging.getLogger(__name__)

STATIC_TABLES = ["nation", "region"]


@no_type_check
def batch(iterable, n=1):
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx : min(ndx + n, length)]


def gen_csv(part_idx: int, cachedir: str, scale_factor: float, num_parts: int) -> None:
    subprocess.check_output(
        shlex.split(f"./dbgen -v -f -s {scale_factor} -S {part_idx} -C {num_parts}"),
        cwd=str(tpch_dbgen),
    )


def pipelined_data_generation(
    scratch_dir: str,
    scale_factor: float,
    num_batches: int,
    aws_s3_sync_location: str,
    parallelism: int = 4,
    rows_per_file: int = 500_000,
) -> None:
    if aws_s3_sync_location.endswith("/"):
        aws_s3_sync_location = aws_s3_sync_location[:-1]

    base_path = pathlib.Path(scratch_dir) / str(num_batches)
    base_path.mkdir(parents=True, exist_ok=True)

    num_dbgen_partitions = num_batches * parallelism
    for batch_idx, part_indices in enumerate(
        batch(range(1, num_dbgen_partitions + 1), n=parallelism)
    ):
        logger.info("Partition %s: Generating CSV files", part_indices)
        with Pool(parallelism) as process_pool:
            process_pool.starmap(
                gen_csv,
                [
                    (part_idx, base_path, scale_factor, num_dbgen_partitions)
                    for part_idx in part_indices
                ],
            )

        csv_files = glob.glob(f"{tpch_dbgen}/*.tbl*")  # noqa: PTH207
        for f in csv_files:
            shutil.move(f, base_path / pathlib.Path(f).name)

        gen_parquet(base_path, rows_per_file, partitioned=True, batch_idx=batch_idx)
        parquet_files = glob.glob(f"{base_path}/*.parquet")  # noqa: PTH207

        if len(aws_s3_sync_location):
            subprocess.check_output(
                shlex.split(
                    f'aws s3 sync {scratch_dir} {aws_s3_sync_location}/scale-{scale_factor} --exclude "*" --include "*.parquet"'
                )
            )
            for parquet_file in parquet_files:
                os.remove(parquet_file)  # noqa: PTH107
        for table_file in glob.glob(f"{base_path}/*.tbl*"):  # noqa: PTH207
            os.remove(table_file)  # noqa: PTH107


# Source tables contained in the schema for TPC-H. For more information, check -
# https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf
table_columns = {
    "customer": [
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment",
    ],
    "lineitem": [
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "comments",
    ],
    "nation": [
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment",
    ],
    "orders": [
        "o_orderkey",
        "o_custkey",
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority",
        "o_comment",
    ],
    "part": [
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment",
    ],
    "partsupp": [
        "ps_partkey",
        "ps_suppkey",
        "ps_availqty",
        "ps_supplycost",
        "ps_comment",
    ],
    "region": [
        "r_regionkey",
        "r_name",
        "r_comment",
    ],
    "supplier": [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment",
    ],
}


def gen_parquet(
    base_path: pathlib.Path,
    rows_per_file: int = 500_000,
    partitioned: bool = False,
    batch_idx: int = 0,
) -> None:
    for table_name, columns in table_columns.items():
        if table_name in STATIC_TABLES and batch_idx != 0:
            continue

        path = base_path / f"{table_name}.tbl*"

        lf = pl.scan_csv(
            path,
            has_header=False,
            separator="|",
            try_parse_dates=True,
            new_columns=columns,
        )

        # Drop empty last column because CSV ends with a separator
        lf = lf.select(columns)

        if partitioned:

            def partition_file_name(ctx: pl.BasePartitionContext) -> pathlib.Path:
                partition = f"{batch_idx}_{ctx.file_idx}"
                (base_path / table_name / partition).mkdir(parents=True, exist_ok=True)  # noqa: B023
                return pathlib.Path(partition) / "part.parquet"

            path = base_path / table_name
            lf.sink_parquet(
                pl.PartitionMaxSize(
                    path, file_path=partition_file_name, max_size=rows_per_file
                )
            )
        else:
            path = base_path / f"{table_name}.parquet"
            lf.sink_parquet(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpch_gen_folder",
        default="data/tables",
        help="Path to generated data folder",
    )
    parser.add_argument(
        "--scale-factor",
        default=settings.scale_factor,
        help="Scale factor to run on",
        type=float,
    )
    parser.add_argument(
        "--rows-per-file",
        default=5_000_000,
        help="Number of rows per parquet file",
        type=int,
    )
    parser.add_argument(
        "--num-batches",
        default=None,
        help="Number of batches used to generate the data",
        type=int,
        nargs="?",
    )
    parser.add_argument(
        "--aws-s3-sync-location",
        default="",
        help="Where (and if) to sync files to in AWS S3",
    )
    parser.add_argument(
        "--parallelism",
        default=8,
        type=int,
        help="How many processes to use to generate the data",
    )
    args = parser.parse_args()

    if args.num_batches is None:
        # Assumes the tables are already created by the Makefile
        gen_parquet(
            pathlib.Path(args.tpch_gen_folder),
            rows_per_file=args.rows_per_file,
            partitioned=False,
        )
    else:
        pipelined_data_generation(
            args.tpch_gen_folder,
            args.scale_factor,
            args.num_batches,
            args.aws_s3_sync_location,
            parallelism=args.parallelism,
            rows_per_file=args.rows_per_file,
        )
