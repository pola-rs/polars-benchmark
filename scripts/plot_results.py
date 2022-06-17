"""This script uses Plotly to visualize benchmark results.

To use this script run

```shell
.venv/bin/python ./scripts/plot_results.py
```
"""
from pathlib import Path

import plotly.express as px
import polars as pl
from plotly.graph_objects import Figure

# expected filepath of benchmark results csv
# assumes csv is in parent directory
FILENAME = "timings.csv"
DEFAULT_CSV_FILEPATH = Path(__file__).parent.parent.resolve() / FILENAME

# colors for each bar
COLORS = {
    "polars": "#AFECF4",
    "dask": "#3EC1CD",
    "pandas": "#0C3B5F",
}

# default base template for plot's theme
DEFAULT_THEME = "plotly_dark"

# other configuration
BAR_TYPE = "group"
LABEL_UPDATES = {"x": "query", "y": "seconds", "color": "lib"}


def plot(
    df: pl.DataFrame,
    x: str = "query_no",
    y: str = "duration[s]",
    group: str = "solution",
) -> Figure:
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
    # build ploty figure object
    fig = px.histogram(
        x=df[x],
        y=df[y],
        color=df[group],
        barmode=BAR_TYPE,
        template=DEFAULT_THEME,
        color_discrete_map=COLORS,
        labels=LABEL_UPDATES,
    )

    return fig


if __name__ == "__main__":
    df = pl.read_csv(DEFAULT_CSV_FILEPATH)
    fig = plot(df)

    # display the object using available environment context
    fig.show()
