from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

type FILE_FORMAT = Literal["skip", "parquet", "feather"]


class Library(BaseModel):
    name: str
    version: str = "latest"
    parameters: dict[str, Any] = {}


class Paths(BaseSettings):
    tables: Path = Path("data/tables")
    answers: Path = Path("data/answers")

    timings: Path | None = None
    plots: Path | None = None

    model_config = SettingsConfigDict(
        env_prefix="tpch_path_", env_file=".env", extra="ignore"
    )


class Settings(BaseSettings):
    scale_factor: int = 1
    file_formats: set[FILE_FORMAT] = {"skip"}

    print_query_output: bool = False

    paths: Paths = Paths()

    libraries: list[Library] = [
        Library(name="polars"),
        Library(name="duckdb"),
        Library(name="pandas"),
        Library(name="pyspark"),
    ]

    model_config = SettingsConfigDict(
        env_prefix="tpch_", env_file=".env", extra="ignore"
    )
