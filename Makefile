SHELL=/bin/bash
PYTHON=.venv/bin/python

.venv:
	@python -m venv .venv
	@.venv/bin/pip install -U pip
	@.venv/bin/pip install --no-cache-dir -r requirements.txt

clean:
	$(MAKE) -C tpch-dbgen clean


tables_scale_1: .venv
	#$(MAKE) -C tpch all
	cd tpch-dbgen && ./dbgen -vf -s 1 && cd ..
	mkdir "tables_scale_1"
	mv tpch-dbgen/*.tbl tables_scale_1/
	.venv/bin/python prepare_files.py


run: .venv tables_scale_1
	.venv/bin/python -m polars_queries.q1
	.venv/bin/python -m polars_queries.q2

pre-commit:
	.venv/bin/python -m isort .
	.venv/bin/python -m black .
