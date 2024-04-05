import re
import sys
from pathlib import Path
from subprocess import run
from typing import Any

from linetimer import CodeTimer

from settings import Settings

settings = Settings()


def append_row(
    solution: str, q: str, secs: float, version: str, success: bool = True
) -> None:
    path = settings.paths.timings
    if not path.parent.exists():
        path.parent.mkdir(parents=True)

    with path.open("a") as f:
        if f.tell() == 0:
            f.write("solution,version,query_no,duration[s],include_io,success\n")
        f.write(
            f"{solution},{version},{q},{secs},{settings.run.include_io},{success}\n"
        )


def on_second_call(func: Any) -> Any:
    def helper(*args: Any, **kwargs: Any) -> Any:
        helper.calls += 1  # type: ignore[attr-defined]

        # first call is outside the function
        # this call must set the result
        if helper.calls == 1:  # type: ignore[attr-defined]
            # include IO will compute the result on the 2nd call
            if not settings.run.include_io:
                helper.result = func(*args, **kwargs)  # type: ignore[attr-defined]
            return helper.result  # type: ignore[attr-defined]

        # second call is in the query, now we set the result
        if settings.run.include_io and helper.calls == 2:  # type: ignore[attr-defined]
            helper.result = func(*args, **kwargs)  # type: ignore[attr-defined]

        return helper.result  # type: ignore[attr-defined]

    helper.calls = 0  # type: ignore[attr-defined]
    helper.result = None  # type: ignore[attr-defined]

    return helper


def execute_all(package_name: str) -> None:
    print(settings.model_dump_json())

    expr = re.compile(r"q(\d+).py$")
    num_queries = 0

    cwd = Path(__file__).parent
    for file in (cwd / package_name).iterdir():
        g = expr.search(str(file))
        if g is not None:
            num_queries = max(int(g.group(1)), num_queries)

    with CodeTimer(name=f"Overall execution of ALL {package_name} queries", unit="s"):
        for i in range(1, num_queries + 1):
            run([sys.executable, "-m", f"queries.{package_name}.q{i}"])
