import re
import sys
from pathlib import Path
from subprocess import run
from typing import Any

from linetimer import CodeTimer

from settings import Settings

settings = Settings()


def log_query_timing(
    solution: str, version: str, query_number: int, time: float
) -> None:
    settings.paths.timings.mkdir(parents=True, exist_ok=True)

    with (settings.paths.timings / settings.paths.timings_filename).open("a") as f:
        if f.tell() == 0:
            f.write(
                "solution,version,query_number,duration[s],include_io,scale_factor\n"
            )

        line = (
            ",".join(
                [
                    solution,
                    version,
                    str(query_number),
                    str(time),
                    str(settings.run.include_io),
                    str(settings.scale_factor),
                ]
            )
            + "\n"
        )
        f.write(line)


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


def get_query_numbers(library_name: str) -> list[int]:
    """Get the query numbers that are implemented for the given library."""
    query_numbers = []

    path = Path(__file__).parent / library_name
    expr = re.compile(r"q(\d+).py$")

    for file in path.iterdir():
        match = expr.search(str(file))
        if match is not None:
            query_numbers.append(int(match.group(1)))

    return sorted(query_numbers)
