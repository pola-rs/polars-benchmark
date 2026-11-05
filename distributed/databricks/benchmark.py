"""PDS-H benchmark on Databricks, using the Spark SQL queries from ../spark.

Runs as a `spark_python_task`; submitted by `run.sh`.
"""

import argparse
import json
import time
import types
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]


def load_queries(spec_path: str) -> dict[int, types.ModuleType]:
    with Path(spec_path).open() as f:
        docs = list(yaml.safe_load_all(f))
    data = next(
        d["data"]
        for d in docs
        if d
        and d.get("kind") == "ConfigMap"
        and d["metadata"]["name"] == "spark-queries"
    )
    queries = {}
    for filename, source in data.items():
        n = int(filename.removeprefix("q").removesuffix(".py"))
        mod = types.ModuleType(f"q{n}")
        exec(source, mod.__dict__)
        queries[n] = mod
    return queries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", required=True)
    parser.add_argument("--queries-file", required=True)
    parser.add_argument("--queries", default="1-22")
    args = parser.parse_args()

    selected = []
    for part in args.queries.split(","):
        lo, _, hi = part.partition("-")
        selected.extend(range(int(lo), int(hi or lo) + 1))

    queries = load_queries(args.queries_file)
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.databricks.io.cache.enabled", "false")

    data_path = args.data_path.rstrip("/")

    for table in TABLES:
        spark.read.parquet(f"{data_path}/{table}").createOrReplaceTempView(table)

    timings: dict[int, float | None] = {}
    for n in selected:
        mod = queries[n]
        spark.catalog.clearCache()
        spark.sql("CLEAR CACHE")
        print(f"Submit q{n}", flush=True)
        t0 = time.time()
        try:
            if hasattr(mod, "setup"):
                spark.sql(mod.setup())
            spark.sql(mod.q()).show()
            if hasattr(mod, "teardown"):
                spark.sql(mod.teardown())
            timings[n] = time.time() - t0
            print(f"Query q{n} executed in: {timings[n]:.2f} seconds\n", flush=True)
        except Exception as e:
            timings[n] = None
            print(f"Query q{n} failed: {e}\n", flush=True)

    for n, t in timings.items():
        print(f"q{n:<2}    -" if t is None else f"q{n:<2} {t:6.2f}")
    ok = [t for t in timings.values() if t is not None]
    print(f"total {sum(ok):6.2f}")
    print("RESULTS_JSON " + json.dumps({f"q{n}": t for n, t in timings.items()}))


if __name__ == "__main__":
    main()
