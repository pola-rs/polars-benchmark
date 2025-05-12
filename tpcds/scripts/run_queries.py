#!/usr/bin/env python3
"""
scripts/run_queries.py
Usage:
  # Run all queries:
  ./run_queries.py --all
  # Run specific ones:
  ./run_queries.py q1 q3
"""

import argparse
import subprocess
import sys
import time
import polars as pl
from pathlib import Path
from datetime import datetime


def discover_queries(queries_dir: Path):
    """Return a dict mapping module-names (e.g. 'q1') to script-paths."""
    scripts = {}
    for py in sorted(queries_dir.glob("q*.py")):
        name = py.stem
        scripts[name] = str(py)
    return scripts


def ensure_results_dir(queries_dir: Path) -> Path:
    """Ensure the results directory exists under queries/ and return its path."""
    results_dir = queries_dir / "results"
    results_dir.mkdir(exist_ok=True)
    return results_dir


def run_query_with_timing(query_name: str, script_path: str):
    """Run a query script and measure its execution time.

    Args:
        query_name: Name of the query (e.g. 'q1')
        script_path: Path to the script file

    Returns:
        tuple: (subprocess.CompletedProcess, execution_time in seconds)
    """
    print(f"\n Running {query_name}")
    start_time = time.time()
    res = subprocess.run([sys.executable, script_path])
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"✓ {query_name} completed in {execution_time:.2f} seconds")

    return res, execution_time


def write_results_to_csv(results: list, results_dir: Path):
    """Write query execution results to a CSV file using Polars.

    Args:
        results: List of dictionaries with timing results
        results_dir: Directory to store the results
    """
    # Create a polars DataFrame from the results
    df = pl.DataFrame(results)

    # Generate a filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"TPCDS_query_results_{timestamp}.csv"
    filepath = results_dir / filename

    # Write the DataFrame to a CSV file
    df.write_csv(filepath)
    print(f"Results written to {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description="Discover and run query scripts from queries/"
    )
    parser.add_argument(
        "queries",
        nargs="*",
        help="Which queries to run (script names without .py). If omitted or --all, runs everything.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all discovered queries",
    )
    args = parser.parse_args()

    base = Path(__file__).parent.parent  # repo root
    queries_dir = base / "queries"
    scripts = discover_queries(queries_dir)

    # Ensure results directory exists
    results_dir = ensure_results_dir(queries_dir)

    # decide what to run
    if args.all or not args.queries:
        to_run = list(scripts.keys())
    else:
        to_run = args.queries

    # sanity check
    for q in to_run:
        if q not in scripts:
            print(f"Unknown query: {q}", file=sys.stderr)
            sys.exit(1)

    # collect timing results
    timing_results = []
    total_start_time = time.time()

    for q in to_run:
        script = scripts[q]
        res, execution_time = run_query_with_timing(q, script)

        # Store the result
        timing_results.append(
            {
                "query_name": q,
                "execution_time_seconds": execution_time,
            }
        )

        if res.returncode != 0:
            print(f" {q} exited with code {res.returncode}", file=sys.stderr)
            sys.exit(res.returncode)

    total_time = time.time() - total_start_time
    print(f"\n All done. Total execution time: {total_time:.2f} seconds")

    write_results_to_csv(timing_results, results_dir)


if __name__ == "__main__":
    main()
