import sys
from pathlib import Path

import polars as pl

SCALE_FACTOR = int(sys.argv[1])
ROOT_DIR = Path(__file__).parent.parent
TABLES_DIR = ROOT_DIR / "data" / "tables" / f"scale-{SCALE_FACTOR}"

h_nation = [
    "n_nationkey",
    "n_name",
    "n_regionkey",
    "n_comment",
]
h_region = [
    "r_regionkey",
    "r_name",
    "r_comment",
]
h_part = [
    "p_partkey",
    "p_name",
    "p_mfgr",
    "p_brand",
    "p_type",
    "p_size",
    "p_container",
    "p_retailprice",
    "p_comment",
]
h_supplier = [
    "s_suppkey",
    "s_name",
    "s_address",
    "s_nationkey",
    "s_phone",
    "s_acctbal",
    "s_comment",
]
h_partsupp = [
    "ps_partkey",
    "ps_suppkey",
    "ps_availqty",
    "ps_supplycost",
    "ps_comment",
]
h_customer = [
    "c_custkey",
    "c_name",
    "c_address",
    "c_nationkey",
    "c_phone",
    "c_acctbal",
    "c_mktsegment",
    "c_comment",
]
h_orders = [
    "o_orderkey",
    "o_custkey",
    "o_orderstatus",
    "o_totalprice",
    "o_orderdate",
    "o_orderpriority",
    "o_clerk",
    "o_shippriority",
    "o_comment",
]
h_lineitem = [
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
]


for name in [
    "nation",
    "region",
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
]:
    print(f"Processing table: {name}")

    lf = pl.scan_csv(
        TABLES_DIR / f"{name}.tbl",
        has_header=False,
        separator="|",
        try_parse_dates=True,
        new_columns=eval(f"h_{name}"),
    )

    # TODO: Remove this cast
    lf = lf.with_columns(pl.col(pl.Date).cast(pl.Datetime))

    lf.sink_parquet(TABLES_DIR / f"{name}.parquet")
    lf.sink_ipc(TABLES_DIR / f"{name}.ipc")
