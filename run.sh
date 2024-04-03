export TPCH_PATH_TIMINGS=results/timings.csv
export TPCH_PATH_PLOTS=results/plots
export TPCH_SCALE_FACTOR=1

echo run with cached IO
make tables
make run-all
make plot

echo run with IO
export INCLUDE_IO=1
make run-all
make plot
