# M5 Forecasting Feature Engineering

The [M5 Forecasting Competition](https://www.sciencedirect.com/science/article/pii/S0169207021001874) was held on Kaggle in 2020,
and top solutions generally featured a lot of heavy feature engineering.

Participants typically used pandas (Polars was only just getting started at the time), so here we benchmark how long it have
taken to do the same feature engineering with Polars (and, coming soon, DuckDB).

We believe this to be a useful task to benchmark, because:

- the competition was run on real-world Walmart data
- the operations we're benchmarking are from the winning solution, so evidently they were doing something right

The original code can be found here: https://github.com/Mcompetitions/M5-methods. We run part of the prepocessing
functions from the top solution "A1". The code is generally kept as-is, with some minor modifications to deal
with pandas syntax updates which happened in the last 2 years.

## Running the benchmark

**Data**: download the output files from https://www.kaggle.com/code/marcogorelli/winning-solution-preprocessing.
Place them in a `data` folder here.

**Run**:

- `python pandas_queries.py`
- `python polars_queries.py`

