export LOG_TIMINGS=1
export WRITE_PLOT=1
export SCALE_FACTOR=1

echo run with cached IO
make tables
make run-all
make plot

echo run with IO
export INCLUDE_IO=1
make run-all
make plot
