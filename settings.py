from pathlib import Path
from typing import Literal, TypeAlias

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

IoType: TypeAlias = Literal["skip", "parquet", "feather", "csv"]


class Paths(BaseSettings):
    answers: Path = Path("data/answers")
    tables: Path = Path("data/tables")

    timings: Path = Path("output/run")
    timings_filename: str = "timings.csv"

    plots: Path = Path("output/plot")

    model_config = SettingsConfigDict(
        env_prefix="path_", env_file=".env", extra="ignore"
    )


class Run(BaseSettings):
    io_type: IoType = "parquet"

    log_timings: bool = False
    show_results: bool = False
    check_results: bool = False  # Only available for SCALE_FACTOR=1

    polars_show_plan: bool = False
    polars_eager: bool = False
    polars_streaming: bool = False

    modin_memory: int = 8_000_000_000  # Tune as needed for optimal performance

    spark_driver_memory: str = "2g"  # Tune as needed for optimal performance
    spark_executor_memory: str = "1g"  # Tune as needed for optimal performance
    spark_log_level: str = "ERROR"

    @computed_field  # type: ignore[misc]
    @property
    def include_io(self) -> bool:
        return self.io_type != "skip"

    model_config = SettingsConfigDict(
        env_prefix="run_", env_file=".env", extra="ignore"
    )


class Plot(BaseSettings):
    show: bool = False
    n_queries: int = 7
    limit_with_io: float = 20.0
    limit_without_io: float = 15.0

    model_config = SettingsConfigDict(
        env_prefix="plot_", env_file=".env", extra="ignore"
    )


class Settings(BaseSettings):
    scale_factor: int = 1

    paths: Paths = Paths()
    plot: Plot = Plot()
    run: Run = Run()

    @computed_field  # type: ignore[misc]
    @property
    def dataset_base_dir(self) -> Path:
        return self.paths.tables / f"scale-{self.scale_factor}"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
