"""Script for visualizing benchmark results using Plotly.

To use this script, run:

```shell
.venv/bin/python -m scripts.plot_bars
```
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import plotly.express as px
import polars as pl

from settings import Settings

if TYPE_CHECKING:
    from plotly.graph_objects import Figure

settings = Settings()

if settings.run.include_io:
    LIMIT = settings.plot.limit_with_io
else:
    LIMIT = settings.plot.limit_without_io

# colors for each bar
COLORS = {
    "polars": "#0075FF",
    "duckdb": "#73BFB8",
    "pandas": "#26413C",
    "dask": "#EFA9AE",
    "pyspark": "#87F7CF",
}


def main() -> None:
    pl.Config.set_tbl_rows(40)

    df = prep_data()
    print(df)
    plot(df)


def prep_data() -> pl.DataFrame:
    lf = pl.scan_csv(settings.paths.timings / settings.paths.timings_filename)

    # Scale factor not used at the moment
    lf = lf.drop("scale_factor")

    # Select timings either with or without IO
    if settings.run.include_io:
        io = pl.col("include_io")
    else:
        io = ~pl.col("include_io")
    lf = lf.filter(io).drop("include_io")

    # Select relevant queries
    lf = lf.filter(pl.col("query_number") <= settings.plot.n_queries)

    # Get the last timing entry per solution/version/query combination
    lf = lf.group_by("solution", "version", "query_number").last()

    # Insert missing query entries
    groups = lf.select("solution", "version").unique()
    queries = pl.LazyFrame({"query_number": range(1, settings.plot.n_queries + 1)})
    groups_queries = groups.join(queries, how="cross")
    lf = groups_queries.join(lf, on=["solution", "version", "query_number"], how="left")
    lf = lf.with_columns(pl.col("duration[s]").fill_null(0))

    # Order the groups
    solution = pl.LazyFrame({"solution": list(COLORS)})
    lf = solution.join(lf, on=["solution"], how="left")

    # Make query number a string
    lf = lf.with_columns(pl.col("query_number").cast(pl.String))

    return lf.collect()


def plot(df: pl.DataFrame) -> Figure:
    """Generate a Plotly Figure of a grouped bar chart displaying benchmark results.

    Parameters
    ----------
    df
        DataFrame containing `x`, `y`, and `group`.
    x
        Column for X Axis. Defaults to "query_number".
    y
        Column for Y Axis. Defaults to "duration[s]".
    group
        Column for group. Defaults to "solution".
    limit
        height limit in seconds

    Returns
    -------
    px.Figure: Plotly Figure (histogram)
    """
    x = df.select(pl.col("query_number").cast(pl.String)).to_series()
    y = df.get_column("duration[s]")
    group = df.select(pl.concat_str("solution", "version")).to_series()

    # build plotly figure object
    fig = px.histogram(
        x=x,
        y=y,
        color=group,
        barmode="group",
        template="plotly_dark",
        color_discrete_map=COLORS,
    )

    fig.update_layout(
        bargroupgap=0.1,
        paper_bgcolor="rgba(41,52,65,1)",
        xaxis_title="Query",
        yaxis_title="Seconds",
        yaxis_range=[0, LIMIT],
        plot_bgcolor="rgba(41,52,65,1)",
        margin={"t": 100},
        legend={
            "orientation": "h",
            "xanchor": "left",
            "yanchor": "top",
            "x": 0.37,
            "y": -0.1,
        },
    )

    # add_annotations(fig, limit, df)

    write_plot_image(fig)

    # display the object using available environment context
    if settings.plot.show:
        fig.show()


def add_annotations(fig: Any, limit: int, df: pl.DataFrame) -> None:
    # order of solutions in the file
    # e.g. ['polar', 'pandas']
    bar_order = (
        df.get_column("solution")
        .unique(maintain_order=True)
        .to_frame()
        .with_row_index()
    )

    # every bar in the plot has a different offset for the text
    start_offset = 10
    offsets = [start_offset + 12 * i for i in range(bar_order.height)]

    # we look for the solutions that surpassed the limit
    # and create a text label for them
    df = (
        df.filter(pl.col("duration[s]") > limit)
        .with_columns(
            pl.format(
                "{} took {} s", "solution", pl.col("duration[s]").cast(pl.Int32)
            ).alias("labels")
        )
        .join(bar_order, on="solution")
        .group_by("query_number")
        .agg(pl.col("labels"), pl.col("index").min())
        .with_columns(pl.col("labels").list.join(",\n"))
    )

    # then we create a dictionary similar to something like this:
    #     anno_data = {
    #         "q1": (offset, "label"),
    #         "q3": (offset, "label"),
    #     }

    if df.height > 0:
        anno_data = {
            v[0]: (offsets[int(v[1])], v[2])
            for v in df.select("query_number", "index", "labels")
            .transpose()
            .to_dict(as_series=False)
            .values()
        }
    else:
        # a dummy with no text
        anno_data = {"q1": (0, "")}

    for q_name, (x_shift, anno_text) in anno_data.items():
        fig.add_annotation(
            align="right",
            x=q_name,
            y=LIMIT,
            xshift=x_shift,
            yshift=30,
            font={"color": "white"},
            showarrow=False,
            text=anno_text,
        )


def write_plot_image(fig: Any) -> None:
    path = settings.paths.plots
    if not path.exists():
        path.mkdir()

    if settings.run.include_io:
        file_name = "plot_with_io.html"
    else:
        file_name = "plot_without_io.html"

    fig.write_html(path / file_name)


if __name__ == "__main__":
    main()
