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

run_polars: .venv
	.venv/bin/python -m polars_queries.q1
	.venv/bin/python -m polars_queries.q2
	.venv/bin/python -m polars_queries.q3
	.venv/bin/python -m polars_queries.q4
	.venv/bin/python -m polars_queries.q5
	.venv/bin/python -m polars_queries.q6
	.venv/bin/python -m polars_queries.q7

run_pandas: .venv
	.venv/bin/python -m pandas_queries.q1
	.venv/bin/python -m pandas_queries.q2
	.venv/bin/python -m pandas_queries.q3
	.venv/bin/python -m pandas_queries.q4
	.venv/bin/python -m pandas_queries.q5
	.venv/bin/python -m pandas_queries.q6
	.venv/bin/python -m pandas_queries.q7

run_dask: .venv
	.venv/bin/python -m dask_queries.q1
	.venv/bin/python -m dask_queries.q2
	.venv/bin/python -m dask_queries.q3
	.venv/bin/python -m dask_queries.q4
	.venv/bin/python -m dask_queries.q5
	.venv/bin/python -m dask_queries.q6
	.venv/bin/python -m dask_queries.q7

run_all: run_polars run_pandas run_dask

pre-commit:
	.venv/bin/python -m isort .
	.venv/bin/python -m black .
