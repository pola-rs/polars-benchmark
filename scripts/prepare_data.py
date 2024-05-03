import pyarrow.csv as pacsv
import pyarrow.parquet as papq

from settings import Settings

settings = Settings()


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

    ##
    # Polars Parquet writes are currently writing extremely fragmented Parquet files
    # Thus we use PyArrow to process the data instead.
    # See: https://github.com/pola-rs/tpch/issues/123
    ##
    data = pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(column_names=columns + [""]),
        parse_options=pacsv.ParseOptions(delimiter="|"),
    )
    data = data.select(columns)
    papq.write_table(data, settings.dataset_base_dir / f"{table_name}.parquet")
    pacsv.write_csv(data, settings.dataset_base_dir / f"{table_name}.csv")

    # IPC currently not relevant
    # lf.sink_ipc(base_path / f"{table_name}.feather")
