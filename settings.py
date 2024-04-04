from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Paths(BaseSettings):
    timings: Path = Path("output/run/timings.csv")
    plots: Path = Path("output/plot")

    model_config = SettingsConfigDict(
        env_prefix="tpch_path_", env_file=".env", extra="ignore"
    )


class Run(BaseSettings):
    include_io: bool = False

    model_config = SettingsConfigDict(
        env_prefix="tpch_run_", env_file=".env", extra="ignore"
    )


class Plot(BaseSettings):
    show: bool = False
    n_queries: int = 8
    limit_with_io: int = 15
    limit_without_io: int = 15

    model_config = SettingsConfigDict(
        env_prefix="tpch_plot_", env_file=".env", extra="ignore"
    )


class Settings(BaseSettings):
    scale_factor: int = 1

    paths: Paths = Paths()
    plot: Plot = Plot()
    run: Run = Run()

    model_config = SettingsConfigDict(
        env_prefix="tpch_", env_file=".env", extra="ignore"
    )
