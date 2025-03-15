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

import polars as pl

from settings import Settings

tpch_dbgen = pathlib.Path(__file__).parent.parent / "tpch-dbgen"


settings = Settings()


logger = logging.getLogger(__name__)

STATIC_TABLES = ["nation", "region"]


def batch(iterable, n=1):
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx: min(ndx + n, length)]


def gen_csv(part_idx: int, cachedir: str, scale_factor: float, num_parts: int):
    subprocess.check_output(
        shlex.split(f"./dbgen -v -f -s {scale_factor} -S {part_idx} -C {num_parts}"),
        cwd=str(tpch_dbgen),
    )


def pipelined_data_generation(
    scratch_dir: str, scale_factor: float, num_parts: int, aws_s3_sync_location: str,
    parallelism: int = 4,
    rows_per_file: int = 500_000
):
    assert num_parts > 1, "script should only be used if num_parts > 1"

    cachedir = pathlib.Path(scratch_dir) / (f"{scale_factor:.1f}").replace(".", "_") / str(num_parts)
    cachedir.mkdir(parents=True, exist_ok=True)
    # shutil.copytree(tpch_dbgen, cachedir, dirs_exist_ok=True)

    for i, part_indices in enumerate(batch(range(1, num_parts + 1), n=parallelism)):
        logger.info("Partition %s: Generating CSV files", part_indices)
        with Pool(parallelism) as process_pool:
            process_pool.starmap(gen_csv, [(part_idx, cachedir, scale_factor, num_parts) for part_idx in part_indices])

        csv_files = glob.glob(f"{tpch_dbgen}/*.tbl*")
        for f in csv_files:
            shutil.move(f, cachedir / pathlib.Path(f).name)


        gen_parquet(cachedir, rows_per_file)
        parquet_files = glob.glob(f"{cachedir}/*.parquet")

        # # Exclude static tables except for first iteration
        exclude_static_tables = "" if i == 0 else " ".join([f'--exclude "*/{tbl}/*"' for tbl in STATIC_TABLES])
        subprocess.check_output(
            shlex.split(
                f'aws s3 sync {scratch_dir} {aws_s3_sync_location} --exclude "*" --include "*.parquet" {exclude_static_tables}'
            )
        )
        for parquet_file in parquet_files:
            os.remove(parquet_file)
        for table_file in glob.glob(f"{cachedir}/*.tbl*"):
            os.remove(table_file)




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

def gen_parquet(base_path: pathlib.Path,
    rows_per_file: int = 500_000
                ):
    for table_name, columns in table_columns.items():
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
        lf.sink_parquet(pl.PartitionMaxSize(base_path / f"{table_name}{{part}}.parquet", max_size=rows_per_file))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpch_gen_folder",
        default="data/tables2",
        help="Path to the folder containing the TPCH dbgen tool and generated data",
    )
    parser.add_argument("--scale-factor", default=0.1, help="Scale factor to run on in GB", type=float)
    parser.add_argument("--rows-per-file", default=500_000, help="Number of rows per parquet file", type=int)
    parser.add_argument(
        "--num-parts", default=32, help="Number of parts to generate", type=int
    )
    parser.add_argument(
        "--aws-s3-sync-location",
        default="s3://<>uncompressed/tpch-dbgen/",
        help="Where to sync files to in AWS S3",
    )
    parser.add_argument(
        "--parallelism",
        default=8,
        type=int,
        help="How many processes to use to generate the data",
    )
    args = parser.parse_args()
    pipelined_data_generation(
        args.tpch_gen_folder, args.scale_factor, args.num_parts, args.aws_s3_sync_location, parallelism=args.parallelism
    )
