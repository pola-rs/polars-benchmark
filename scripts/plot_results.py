"""This script uses Plotly to visualize benchmark results.

To use this script run

```shell
.venv/bin/python ./scripts/plot_results.py
```
"""

import os

import plotly.express as px
import polars as pl

from common_utils import DEFAULT_PLOTS_DIR, INCLUDE_IO, TIMINGS_FILE, WRITE_PLOT

# colors for each bar
COLORS = {
    "polars": "#f7c5a0",
    "dask": "#87f7cf",
    "pandas": "#72ccff",
    # "modin": "#d4a4eb",
}

# default base template for plot's theme
DEFAULT_THEME = "plotly_dark"

# other configuration
BAR_TYPE = "group"
LABEL_UPDATES = {
    "x": "query",
    "y": "seconds",
    "color": "Solution",
    "pattern_shape": "Solution",
}


def add_annotations(fig, limit: int, df: pl.DataFrame):
    # order of solutions in the file
    # e.g. ['polar', 'pandas', 'dask']
    bar_order = (
        df["solution"].unique(maintain_order=True).to_frame().with_row_count("index")
    )

    # every bar in the plot has a different offset for the text
    start_offset = 10
    offsets = [start_offset + 12 * i for i in range(0, bar_order.height)]

    # we look for the solutions that surpassed the limit
    # and create a text label for them
    df = (
        df.filter(pl.col("duration[s]") > limit)
        .with_columns(
            pl.when(pl.col("success"))
            .then(
                pl.format(
                    "{} took {} s", "solution", pl.col("duration[s]").cast(pl.Int32)
                ).alias("labels")
            )
            .otherwise(pl.format("{} had an internal error", "solution"))
        )
        .join(bar_order, on="solution")
        .groupby("query_no")
        .agg([pl.col("labels"), pl.col("index").min()])
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
            for v in df.select(["query_no", "index", "labels"])
            .transpose()
            .to_dict(False)
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
            font=dict(color="white"),
            showarrow=False,
            text=anno_text,
        )


def write_plot_image(fig):
    if not os.path.exists(DEFAULT_PLOTS_DIR):
        os.mkdir(DEFAULT_PLOTS_DIR)

    file_name = f"plot_with_io.html" if INCLUDE_IO else "plot_without_io.html"
    fig.write_html(os.path.join(DEFAULT_PLOTS_DIR, file_name))


def plot(
    df: pl.DataFrame,
    x: str = "query_no",
    y: str = "duration[s]",
    group: str = "solution",
    limit: int = 120,
):
    """Generate a Plotly Figure of a grouped bar chart diplaying
    benchmark results from a DataFrame.

    Args:
        df (pl.DataFrame): DataFrame containing `x`, `y`, and `group`.
        x (str, optional): Column for X Axis. Defaults to "query_no".
        y (str, optional): Column for Y Axis. Defaults to "duration[s]".
        group (str, optional): Column for group. Defaults to "solution".
        limit: height limit in seconds

    Returns:
        px.Figure: Plotly Figure (histogram)
    """

    # build plotly figure object
    fig = px.histogram(
        x=df[x],
        y=df[y],
        color=df[group],
        barmode=BAR_TYPE,
        template=DEFAULT_THEME,
        color_discrete_map=COLORS,
        pattern_shape=df[group],
        labels=LABEL_UPDATES,
    )

    fig.update_layout(
        bargroupgap=0.1,
        paper_bgcolor="rgba(41,52,65,1)",
        yaxis_range=[0, limit],
        plot_bgcolor="rgba(41,52,65,1)",
        margin=dict(t=100),
        legend=dict(orientation="h", xanchor="left", yanchor="top", x=0.37, y=-0.1),
    )

    add_annotations(fig, limit, df)

    if WRITE_PLOT:
        write_plot_image(fig)

    # display the object using available environment context
    fig.show()


if __name__ == "__main__":
    print("write plot:", WRITE_PLOT)

    e = pl.lit(True)
    max_query = 8

    if INCLUDE_IO:
        LIMIT = 15
        e = e & pl.col("include_io") & ~(pl.col("solution") == "vaex_feather")
    else:
        LIMIT = 15
        e = e & ~pl.col("include_io")

    df = (
        pl.scan_csv(TIMINGS_FILE)
        .filter(e)
        # filter the max query to plot
        .filter((pl.col("query_no").str.extract("q(\d+)", 1).cast(int) <= max_query))
        # create a version no
        .with_columns(
            [
                pl.when(pl.col("success")).then(pl.col("duration[s]")).otherwise(0),
                pl.format("{}-{}", "solution", "version").alias("solution-version"),
            ]
        )
        # ensure we get the latest version
        .sort(["solution", "version"])
        .groupby(["solution", "query_no"], maintain_order=True)
        .last()
        .collect()
    )
    order = pl.DataFrame(
        {
            "solution": [
                "polars",
                "duckdb",
                "pandas",
                "dask",
                "spark",
                "vaex_parquet",
                "modin",
            ]
        }
    )
    df = order.join(df, on="solution", how="left")

    plot(df, limit=LIMIT, group="solution-version")
