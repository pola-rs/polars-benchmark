import os
import re
import sys
from pathlib import Path
from subprocess import run
from typing import Any

from linetimer import CodeTimer

INCLUDE_IO = bool(os.environ.get("INCLUDE_IO", False))
SHOW_RESULTS = bool(os.environ.get("SHOW_RESULTS", False))
LOG_TIMINGS = bool(os.environ.get("LOG_TIMINGS", False))
SCALE_FACTOR = os.environ.get("SCALE_FACTOR", "1")
WRITE_PLOT = bool(os.environ.get("WRITE_PLOT", False))
FILE_TYPE = os.environ.get("FILE_TYPE", "parquet")

print("include io:", INCLUDE_IO)
print("show results:", SHOW_RESULTS)
print("log timings:", LOG_TIMINGS)
print("file type:", FILE_TYPE)


CWD = Path(__file__).parent
ROOT = CWD.parent
DATASET_BASE_DIR = ROOT / "data" / "tables" / f"scale-{SCALE_FACTOR}"
ANSWERS_BASE_DIR = ROOT / "data" / "answers"
TIMINGS_FILE = ROOT / os.environ.get("TIMINGS_FILE", "timings.csv")
DEFAULT_PLOTS_DIR = ROOT / "plots"


def append_row(
    solution: str, q: str, secs: float, version: str, success: bool = True
) -> None:
    with TIMINGS_FILE.open("a") as f:
        if f.tell() == 0:
            f.write("solution,version,query_no,duration[s],include_io,success\n")
        f.write(f"{solution},{version},{q},{secs},{INCLUDE_IO},{success}\n")


def on_second_call(func: Any) -> Any:
    def helper(*args: Any, **kwargs: Any) -> Any:
        helper.calls += 1  # type: ignore[attr-defined]

        # first call is outside the function
        # this call must set the result
        if helper.calls == 1:  # type: ignore[attr-defined]
            # include IO will compute the result on the 2nd call
            if not INCLUDE_IO:
                helper.result = func(*args, **kwargs)  # type: ignore[attr-defined]
            return helper.result  # type: ignore[attr-defined]

        # second call is in the query, now we set the result
        if INCLUDE_IO and helper.calls == 2:  # type: ignore[attr-defined]
            helper.result = func(*args, **kwargs)  # type: ignore[attr-defined]

        return helper.result  # type: ignore[attr-defined]

    helper.calls = 0  # type: ignore[attr-defined]
    helper.result = None  # type: ignore[attr-defined]

    return helper


def execute_all(solution: str) -> None:
    package_name = f"{solution}"

    expr = re.compile(r"q(\d+).py$")
    num_queries = 0
    for file in (CWD / package_name).iterdir():
        g = expr.search(str(file))
        if g is not None:
            num_queries = max(int(g.group(1)), num_queries)

    with CodeTimer(name=f"Overall execution of ALL {solution} queries", unit="s"):
        for i in range(1, num_queries + 1):
            run([sys.executable, "-m", f"queries.{package_name}.q{i}"])
