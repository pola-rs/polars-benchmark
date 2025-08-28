from queries.common_utils import execute_all
from queries.exasol.utils import prepare_load_data, prepare_postload, prepare_schema

if __name__ == "__main__":
    # create schema before loading data
    prepare_schema()
    # load data files into Exasol tables
    prepare_load_data()
    # enforce indices and collect statistics after data load
    prepare_postload()
    # run the TPC-H queries
    execute_all("exasol")
