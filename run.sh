export LOG_TIMINGS=1
export WRITE_PLOT=1

echo run with cached IO
make run_all
make plot_results

echo run with IO
export INCLUDE_IO=1
make run_all
make plot_results
