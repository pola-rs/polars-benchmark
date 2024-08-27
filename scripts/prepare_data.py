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

import polars as pl

from settings import Settings

settings = Settings()

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

for table_name, columns in table_columns.items():
    print(f"Processing table: {table_name}")

    path = settings.dataset_base_dir / f"{table_name}.tbl"
    lf = pl.scan_csv(
        path,
        has_header=False,
        separator="|",
        try_parse_dates=True,
        new_columns=columns,
    )

    # Drop empty last column because CSV ends with a separator
    lf = lf.select(columns)

    lf.sink_parquet(settings.dataset_base_dir / f"{table_name}.parquet")
    lf.sink_csv(settings.dataset_base_dir / f"{table_name}.csv")

    # IPC currently not relevant
    # lf.sink_ipc(base_path / f"{table_name}.feather")
