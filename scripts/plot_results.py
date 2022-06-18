"""This script uses Plotly to visualize benchmark results.

To use this script run

```shell
.venv/bin/python ./scripts/plot_results.py
```
"""

import plotly.express as px
import polars as pl

from common_utils import TIMINGS_FILE

# expected filepath of benchmark results csv
# assumes csv is in parent directory

DEFAULT_CSV_FILEPATH = TIMINGS_FILE

# colors for each bar
COLORS = {
    "polars": "#f7c5a0",
    "dask": "#87f7cf",
    "pandas": "#72ccff",
    "modin": "#d4a4eb",
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


def add_annotations(fig):
    anno_data = {
        "q1": (37, "Modin took over<br>2 minutes"),
        "q3": (37, "Modin took over<br>2 minutes"),
        "q4": (22, "Modin and Dask took<br>over 2 minutes"),
        "q5": (22, "Modin and Dask took<br>over 2 minutes"),
        "q7": (10, "Modin, Dask and Pandas<br>took over 2 minutes"),
    }

    for q_name, (x_shift, anno_text) in anno_data.items():
        fig.add_annotation(
            align="right",
            x=q_name,
            y=120,
            xshift=x_shift,
            yshift=30,
            font=dict(color="white"),
            showarrow=False,
            text=anno_text,
        )


def plot(
        df: pl.DataFrame,
        x: str = "query_no",
        y: str = "duration[s]",
        group: str = "solution",
):
    """Generate a Plotly Figure of a grouped bar chart diplaying
    benchmark results from a DataFrame.

    Args:
        df (pl.DataFrame): DataFrame containing `x`, `y`, and `group`.
        x (str, optional): Column for X Axis. Defaults to "query_no".
        y (str, optional): Column for Y Axis. Defaults to "duration[s]".
        group (str, optional): Column for group. Defaults to "solution".

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
        yaxis_range=[0, 120],
        plot_bgcolor="rgba(41,52,65,1)",
        margin=dict(t=100),
        legend=dict(orientation="h", xanchor="left", yanchor="top", x=0.37, y=-0.1),
    )

    add_annotations(fig)

    # display the object using available environment context
    fig.show()


if __name__ == "__main__":
    df = pl.read_csv(DEFAULT_CSV_FILEPATH)
    plot(df)
