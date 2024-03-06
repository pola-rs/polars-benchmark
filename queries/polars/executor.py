from queries.common_utils import execute_all
from queries.settings import Library

if __name__ == "__main__":
    lib = Library(name="polars")
    execute_all(lib)
