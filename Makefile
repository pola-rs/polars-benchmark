SHELL=/bin/bash
PYTHON=.venv/bin/python

.venv:
	@python -m venv .venv
	@.venv/bin/pip install -U pip
	@.venv/bin/pip install --no-cache-dir -r requirements.txt

clean-tpch-dbgen:
	$(MAKE) -C tpch-dbgen clean

clean-venv:
	rm -r .venv

clean-tables:
	rm -r tables_scale_*

clean: clean-tpch-dbgen clean-venv

tables_scale_1: .venv
	$(MAKE) -C tpch-dbgen all
	cd tpch-dbgen && ./dbgen -vf -s 1 && cd ..
	mkdir -p "tables_scale_1"
	mv tpch-dbgen/*.tbl tables_scale_1/
	.venv/bin/python prepare_files.py 1

tables_scale_10: .venv
	$(MAKE) -C tpch-dbgen all
	cd tpch-dbgen && ./dbgen -vf -s 10 && cd ..
	mkdir -p "tables_scale_10"
	mv tpch-dbgen/*.tbl tables_scale_10/
	.venv/bin/python prepare_files.py 10

tables_scale_100: .venv
	$(MAKE) -C tpch-dbgen all
	cd tpch-dbgen && ./dbgen -vf -s 100 && cd ..
	mkdir -p "tables_scale_100"
	mv tpch-dbgen/*.tbl tables_scale_100/
	.venv/bin/python prepare_files.py 100

run_polars: .venv
	.venv/bin/python -m polars_queries.executor

run_pandas: .venv
	.venv/bin/python -m pandas_queries.executor

run_dask: .venv
	.venv/bin/python -m dask_queries.executor

run_modin: .venv
	.venv/bin/python -m modin_queries.executor

run_vaex: .venv
	.venv/bin/python -m vaex_queries.executor

run_spark: .venv
	.venv/bin/python -m spark_queries.executor

run_duckdb: .venv
	.venv/bin/python -m duckdb_queries.executor

plot_results: .venv
	.venv/bin/python -m scripts.plot_results

run_all: run_polars run_pandas run_vaex run_dask run_modin run_spark

pre-commit:
	.venv/bin/python -m isort .
	.venv/bin/python -m black .
