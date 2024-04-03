from queries.common_utils import run_all_queries
from queries.polars.utils import parse_lib_settings

if __name__ == "__main__":
    lib = parse_lib_settings()
    run_all_queries(lib)
