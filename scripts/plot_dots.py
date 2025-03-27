#!/usr/bin/env python3

import argparse
import sys
import textwrap
import warnings

from settings import Settings

try:
    import plotnine as p9
    import polars as pl
    from plotnine.exceptions import PlotnineWarning

    warnings.filterwarnings("ignore", category=PlotnineWarning)
except ImportError:
    print("Please install Polars and Plotnine to use this script.")
    sys.exit(1)

settings = Settings()


def get_styles(exclude_solutions: list[str]) -> pl.DataFrame:
    all_styles = pl.DataFrame(
        data=[
            ["polars", "Polars", "#0075FF", "d", 6.0],
            ["duckdb", "DuckDB", "#73BFB8", "d", 5.5],
            ["pandas", "pandas", "#26413C", "d", 5.0],
            ["dask", "Dask", "#EFA9AE", "d", 4.5],
            ["pyspark", "PySpark", "#87F7CF", "d", 4.5],
        ],
        schema=["solution", "name", "color", "shape", "size"],
    )
    return all_styles.filter(~pl.col("solution").is_in(exclude_solutions))


def parse_queries(s: str) -> list[str]:
    query_numbers: set[int] = set()
    for part in s.split(","):
        if "-" in part:
            start, end = map(int, part.split("-"))
            query_numbers.update(range(start, end + 1))
        else:
            query_numbers.add(int(part))
    return [f"q{x}" for x in sorted(query_numbers)]


def read_csv(filename: str) -> pl.DataFrame:
    if filename == "-":
        df = pl.read_csv(settings.paths.timings / settings.paths.timings_filename)
    else:
        df = pl.read_csv(filename)
    return df


def prepare_timings(
    timings: pl.DataFrame,
    styles: pl.DataFrame,
    exclude_solutions: list[str],
    queries: list[str],
    include_io: bool,
) -> pl.DataFrame:
    return (
        timings.join(styles, on="solution", how="left")
        .filter(
            pl.col("success")
            & pl.col("query_no").is_in(queries)
            & (pl.col("include_io") == include_io)
            & ~pl.col("solution").is_in(exclude_solutions)
        )
        .select(
            pl.col("solution"),
            pl.col("name"),
            (pl.col("name") + " (" + pl.col("version") + ")").alias("name_version"),
            pl.col("query_no").alias("query"),
            pl.col("duration[s]").alias("duration"),
        )
    )


def formulate_caption(
    timings: pl.DataFrame,
    styles: pl.DataFrame,
    queries: list[str],
    no_notes: bool,
    max_duration: float,
    width: float,
) -> str:
    caption = ""

    if not no_notes:
        exceeded_timings = timings.filter(pl.col("duration") > max_duration).select(
            pl.col("name"),
            pl.col("query"),
            (
                pl.lit("took ")
                + pl.col("duration").round(1).cast(pl.Utf8)
                + "s on "
                + pl.col("query")
            ).alias("text"),
        )

        all_combinations_df = styles.select("name").join(
            pl.DataFrame({"query": queries}), how="cross"
        )

        missing_timings = all_combinations_df.join(
            timings, how="anti", on=["name", "query"]
        ).with_columns((pl.lit("failed on ") + pl.col("query")).alias("text"))

        notes_df = pl.concat([exceeded_timings, missing_timings]).sort(
            pl.col("name"), pl.col("query").str.slice(1).cast(pl.Int8)
        )

        notes = []
        for name, group in notes_df.group_by(["name"]):
            texts = group.get_column("text")
            join_char = ", " if len(texts) >= 3 else " "

            if len(texts) >= 2:
                texts[-1] = "and " + texts[-1]

            notes.append(f"{name[0]} {join_char.join(texts)}.")

        if notes:
            caption += f"Note: {' '.join(notes)} "
    caption += "More information: https://www.pola.rs/benchmarks"
    return "\n".join(textwrap.wrap(caption, int(width * 15 - 20)))


def create_plot(
    timings: pl.DataFrame,
    styles: pl.DataFrame,
    queries: list[str],
    caption: str,
    args: argparse.Namespace,
) -> p9.ggplot:
    if args.include_io:
        subtitle = "Results including reading parquet (lower is better)"
    else:
        subtitle = "Results starting from in-memory data (lower is better)"

    theme = {
        "dark": {
            "background_color": "#0d1117",
            "text_color": "#adbac7",
            "line_color": "#999",
        },
        "light": {
            "background_color": "#fff",
            "text_color": "#333",
            "line_color": "#999",
        },
    }

    styles = styles.join(timings, on="solution", how="semi")

    # Cast columns to Enum to make sure the order is correct
    timings = timings.with_columns(
        pl.col("solution").cast(pl.Enum(styles.get_column("solution")))
    )
    name_versions = timings.select(
        pl.col("name_version").sort_by("solution").unique(maintain_order=True)
    ).to_series()
    timings = timings.with_columns(pl.col("name_version").cast(pl.Enum(name_versions)))

    plot = (
        p9.ggplot(
            timings,
            p9.aes(
                x="duration",
                y="query",
                fill="name_version",
                shape="name_version",
                size="name_version",
            ),
        )
        + p9.geom_point(alpha=1, color="black")
        + p9.scale_x_continuous(limits=(0, args.max_duration))
        + p9.scale_y_discrete(limits=queries[::-1])
        + p9.scale_fill_manual(values=styles.get_column("color"))
        + p9.scale_shape_manual(values=styles.get_column("shape"))
        + p9.scale_size_manual(values=styles.get_column("size"))
        + p9.labs(
            title="TPC-H Benchmark",
            subtitle=subtitle,
            caption=caption,
            x="duration (s)",
        )
        + p9.theme_tufte(ticks=False)
        + p9.theme(
            text=p9.element_text(color=theme[args.mode]["text_color"]),
            plot_title=p9.element_text(size=18, weight=800),
            panel_grid_major_y=p9.element_line(color=theme[args.mode]["line_color"]),
            legend_title=p9.element_blank(),
            plot_subtitle=p9.element_text(margin={"b": 20}),
            plot_caption=p9.element_text(
                ha="left", linespacing=2, style="italic", margin={"t": 20}
            ),
            figure_size=(args.width, args.height),
            dpi=args.dpi,
        )
    )

    if not args.transparent:
        plot = plot + p9.theme(
            plot_background=p9.element_rect(
                color=theme[args.mode]["background_color"],
                fill=theme[args.mode]["background_color"],
            )
        )

    return plot


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
        "-q",
        "--queries",
        type=str,
        default="1-8",
        help="Queries to include",
        metavar="<integers and ranges>",
    )
    parser.add_argument(
        "-e",
        "--exclude",
        type=str,
        default="",
        help="Solutions to exclude",
        metavar="<list of solutions>",
    )
    parser.add_argument(
        "-i",
        "--include-io",
        action="store_true",
        help="Include I/O time",
    )
    parser.add_argument(
        "-n",
        "--no-notes",
        action="store_true",
        help="Don't include failed or exceeded timings in caption",
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
        "-t",
        "--transparent",
        action="store_true",
        help="Make figure background transparent",
    )
    parser.add_argument(
        "--width",
        type=float,
        default=8.0,
        help="Figure width",
        metavar="<inch>",
    )
    parser.add_argument(
        "--height",
        type=float,
        default=4.0,
        help="Figure height",
        metavar="<inch>",
    )
    parser.add_argument(
        "--dpi",
        type=float,
        default=200,
        help="Figure DPI",
        metavar="<dpi>",
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

    exclude_solutions = args.exclude.split(",")
    styles = get_styles(exclude_solutions)
    queries = parse_queries(args.queries)
    timings = prepare_timings(
        read_csv(args.csv),
        styles,
        exclude_solutions,
        queries,
        args.include_io,
    )
    caption = formulate_caption(
        timings, styles, queries, args.no_notes, args.max_duration, args.width
    )

    plot = create_plot(timings, styles, queries, caption, args)

    plot.save(settings.paths.plots / args.output)


if __name__ == "__main__":
    main()
