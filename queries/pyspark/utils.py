from __future__ import annotations

import os
import re
import subprocess
import sys
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

import pyspark
from pyspark.sql import SparkSession

from queries.common_utils import (
    check_query_result_pd,
    get_table_path,
    run_query_generic,
)
from settings import Settings

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql import DataFrame

settings = Settings()


def _supported_java_versions() -> tuple[int, ...]:
    """Java versions the installed PySpark can run on, most preferred first."""
    spark_major = int(pyspark.__version__.split(".", 1)[0])
    return (21, 17) if spark_major >= 4 else (17, 11, 8)


def _jdk_home(prefix: str) -> Path:
    """Resolve a JDK install prefix to the directory Java actually calls home."""
    bundle = Path(prefix) / "libexec" / "openjdk.jdk" / "Contents" / "Home"
    return bundle if bundle.is_dir() else Path(prefix)


def _java_major_version(home: Path) -> int | None:
    """Read a Java home's major version, or None if it cannot be determined."""
    try:
        release = (home / "release").read_text()
    except OSError:
        return None

    # e.g. JAVA_VERSION="21.0.12", or JAVA_VERSION="1.8.0_452" for Java 8
    match = re.search(r'^JAVA_VERSION="?(?:1\.)?(\d+)', release, re.MULTILINE)
    return int(match[1]) if match else None


def _candidate_jdk_prefixes(java_version: int) -> Iterator[str]:
    """Paths that may hold a JDK of the given major version, most likely first."""
    try:
        # note: `-F` is required; without it, `java_home` exits 0 and prints
        # the default JVM when no JDK of the requested version is registered
        located = subprocess.run(
            ["/usr/libexec/java_home", "-v", str(java_version), "-F"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        located = ""  # no java_home, or no JDK of this version registered with it
    except OSError as exc:
        located = ""
        print(f"note: could not run java_home: {exc}", file=sys.stderr)

    if located:
        yield located

    yield f"/opt/homebrew/opt/openjdk@{java_version}"  # homebrew, Apple silicon
    yield f"/usr/local/opt/openjdk@{java_version}"  # homebrew, Intel


def _find_java_home() -> Path | None:
    """Locate a JDK that the installed PySpark supports."""
    for java_version in _supported_java_versions():
        for prefix in _candidate_jdk_prefixes(java_version):
            home = _jdk_home(prefix)
            if _java_major_version(home) == java_version:
                if (home / "bin" / "java").exists():
                    return home
                print(f"note: {home} has no bin/java; skipping", file=sys.stderr)
    return None


@cache
def _ensure_java_home() -> None:
    """Point JAVA_HOME at a JDK that the installed PySpark can run on."""
    supported = _supported_java_versions()
    needs = " or ".join(str(v) for v in sorted(supported))

    if current := os.environ.get("JAVA_HOME"):
        # an already-set JAVA_HOME is still checked: pointing it at the system
        # default is the most common way to end up on an unsupported JDK
        found = _java_major_version(Path(current))
        if found in supported:
            return
        print(
            f"warning: JAVA_HOME ({current}) is Java {found or '<unknown>'}, but "
            f"PySpark {pyspark.__version__} requires Java {needs}",
            file=sys.stderr,
        )

    if sys.platform != "darwin":
        # TODO: automatically determine a suitable JAVA_HOME on other platforms
        return

    if home := _find_java_home():
        os.environ["JAVA_HOME"] = str(home)
        print(f"note: using JAVA_HOME={home}", file=sys.stderr)
    else:
        print(
            f"warning: no Java {needs} JDK found; falling back to Spark's own JVM "
            "resolution, which may fail or select an unsupported JDK",
            file=sys.stderr,
        )


def get_or_create_spark() -> SparkSession:
    _ensure_java_home()

    driver_address = settings.run.spark_driver_address
    if driver_address is None and sys.platform == "darwin":
        # Note: bind the local driver to loopback; otherwise Spark resolves the
        # machine hostname, which on macOS often maps to a non-loopback address,
        # causing "TaskResultLost" failures
        driver_address = "127.0.0.1"

    builder = (
        SparkSession.builder.appName("spark_queries")
        .master("local[*]")
        .config("spark.driver.memory", settings.run.spark_driver_memory)
        .config("spark.executor.memory", settings.run.spark_executor_memory)
        .config("spark.log.level", settings.run.spark_log_level)
    )
    if driver_address:
        builder = builder.config("spark.driver.bindAddress", driver_address).config(
            "spark.driver.host", driver_address
        )
    if settings.run.io_type == "network":
        builder = builder.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1"
        )
    return builder.getOrCreate()


def _read_ds(table_name: str) -> DataFrame:
    if settings.run.io_type == "skip":
        # TODO: Persist data in memory before query
        msg = "cannot run PySpark starting from an in-memory representation"
        raise RuntimeError(msg)

    path = get_table_path(table_name)

    if settings.run.io_type in ("parquet", "network"):
        path_str = str(path).replace("s3://", "s3a://")
        df = get_or_create_spark().read.parquet(path_str)
    elif settings.run.io_type == "csv":
        df = get_or_create_spark().read.csv(str(path), header=True, inferSchema=True)
    else:
        msg = f"unsupported file type: {settings.run.io_type!r}"
        raise ValueError(msg)

    df.createOrReplaceTempView(table_name)
    return df


def get_line_item_ds() -> DataFrame:
    return _read_ds("lineitem")


def get_orders_ds() -> DataFrame:
    return _read_ds("orders")


def get_customer_ds() -> DataFrame:
    return _read_ds("customer")


def get_region_ds() -> DataFrame:
    return _read_ds("region")


def get_nation_ds() -> DataFrame:
    return _read_ds("nation")


def get_supplier_ds() -> DataFrame:
    return _read_ds("supplier")


def get_part_ds() -> DataFrame:
    return _read_ds("part")


def get_part_supp_ds() -> DataFrame:
    return _read_ds("partsupp")


def run_query(query_number: int, df: DataFrame) -> None:
    query = df.toPandas
    run_query_generic(
        query, query_number, "pyspark", query_checker=check_query_result_pd
    )
