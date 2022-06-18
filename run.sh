export LOG_TIMINGS=1

echo run with cached IO
make run_all

echo run with IO
export INCLUDE_IO=1
make run_all
