#!/usr/bin/env python3

import sys
import argparse
import fileinput
from typing import List, Tuple

try:
    import polars as pl
    import plotnine as p9
except ImportError:
    print("Please install Polars and Plotnine to use this script.")
    sys.exit(1)


def read_csv(filename: str) -> pl.DataFrame:
    if filename == "-":
        df = pl.read_csv(sys.stdin.buffer)
    else:
        df = pl.read_csv(filename)
    return df


def create_plot(
    df: pl.DataFrame,
    max_duration: float,
    output: str,
    mode: str,
    height: float,
    width: float,
) -> None:
    pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create dot plot from timings CSV file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="-",
        metavar="<csv file>",
        help="CSV file to read (if not specified, reads from stdin)",
    )
    parser.add_argument(
        "-d",
        "--max-duration",
        type=float,
        default=4.0,
        help="Maximum duration",
        metavar="<seconds>",
    )
    parser.add_argument(
        "-i",
        "--include-io",
        action="store_true",
        help="Include I/O time",
    )
    parser.add_argument(
        "-m",
        "--mode",
        type=str,
        choices=["dark", "light"],
        default="dark",
        help="Theme mode",
    )
    parser.add_argument(
        "--height",
        type=float,
        default=4.0,
        help="Figure height",
        metavar="<inch>",
    )
    parser.add_argument(
        "--width",
        type=float,
        default=8.0,
        help="Figure width",
        metavar="<inch>",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="plot.png",
        help="Output file",
        metavar="<png file>",
    )

    args = parser.parse_args()

    df = read_csv(args.csv)
    print(df.head())


if __name__ == "__main__":
    main()
