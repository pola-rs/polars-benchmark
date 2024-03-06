import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from linetimer import CodeTimer

from queries.settings import Library, Settings

settings = Settings()
print(settings.model_dump_json())


DATASET_BASE_DIR = settings.paths.tables / f"scale-{settings.scale_factor}"
TIMINGS_FILE = "timings.csv"


def append_row(solution: str, version: str, query_number: int, time: float) -> None:
    settings.paths.timings.mkdir(exist_ok=True, parents=True)
    with (settings.paths.timings / TIMINGS_FILE).open("a") as f:
        if f.tell() == 0:
            f.write("solution,version,query_number,duration[s]\n")

        line = ",".join([solution, version, str(query_number), str(time)]) + "\n"
        f.write(line)


# def on_second_call(func: Any) -> Any:
#     def helper(*args: Any, **kwargs: Any) -> Any:
#         helper.calls += 1  # type: ignore[attr-defined]

#         # first call is outside the function
#         # this call must set the result
#         if helper.calls == 1:  # type: ignore[attr-defined]
#             # include IO will compute the result on the 2nd call
#             if not INCLUDE_IO:
#                 helper.result = func(*args, **kwargs)  # type: ignore[attr-defined]
#             return helper.result  # type: ignore[attr-defined]

#         # second call is in the query, now we set the result
#         if INCLUDE_IO and helper.calls == 2:  # type: ignore[attr-defined]
#             helper.result = func(*args, **kwargs)  # type: ignore[attr-defined]

#         return helper.result  # type: ignore[attr-defined]

#     helper.calls = 0  # type: ignore[attr-defined]
#     helper.result = None  # type: ignore[attr-defined]

#     return helper


def run_all_queries(lib: Library) -> None:
    executable = _set_up_venv(lib)
    args = _parameters_to_cli_args(lib.parameters)

    query_numbers = get_query_numbers(lib.name)

    with CodeTimer(name=f"Overall execution of ALL {lib.name} queries", unit="s"):
        for i in query_numbers:
            run_query(lib.name, i, args=args, executable=str(executable))


def _set_up_venv(lib: Library) -> Path:
    """Set up a virtual environment for the given library.

    Returns the path to the Python executable.
    """
    venv_path = _create_venv(lib)
    _install_lib(lib, venv_path)
    return venv_path / "bin" / "python"


def _create_venv(lib: Library) -> str:
    """Create a virtual environment for the given library.

    Returns the path to the virtual environment root.
    """
    venv_name = f".venv-{lib.name}-{lib.version}"
    venv_path = settings.paths.venvs / venv_name
    subprocess.run([sys.executable, "-m", "uv", "venv", str(venv_path)])
    return venv_path


def _install_lib(lib: Library, venv_path: Path) -> None:
    """Install the library in the given virtual environment."""
    # Prepare environment
    current_venv = os.environ.pop("VIRTUAL_ENV", None)
    current_conda = os.environ.pop("CONDA_PREFIX", None)
    os.environ["VIRTUAL_ENV"] = str(venv_path)

    # Install
    pip_spec = _get_pip_specifier(lib)
    subprocess.run([sys.executable, "-m", "uv", "pip", "install", pip_spec])
    # TODO: Clean up installing dependencies
    subprocess.run(
        [
            sys.executable,
            "-m",
            "uv",
            "pip",
            "install",
            "linetimer",
            "pydantic",
            "pydantic-settings",
        ]
    )

    # Restore environment
    os.environ.pop("VIRTUAL_ENV")
    if current_venv is not None:
        os.environ["VIRTUAL_ENV"] = current_venv
    if current_conda is not None:
        os.environ["CONDA_PREFIX"] = current_conda


def _get_pip_specifier(lib: Library) -> str:
    if lib.version is None:
        return lib.name
    else:
        return f"{lib.name}=={lib.version}"


def _parameters_to_cli_args(params: dict[str, Any] | None) -> list[str]:
    if params is None:
        return []
    args = []
    for name, value in params.items():
        name = name.replace("_", "-")
        if value is True:
            command = f"--{name}"
        elif value is False:
            command = f"--no-{name}"
        else:
            command = f"--{name}={value}"
        args.append(command)
    return args


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


def run_query(
    library_name: str,
    query_number: int,
    args: list[str] | None = None,
    executable: str = sys.executable,
) -> None:
    """Run a single query for the specified library."""
    module = f"queries.{library_name}.q{query_number}"
    command = [executable, "-m", module]
    if args:
        command += args

    print(command)
    subprocess.run(command)
