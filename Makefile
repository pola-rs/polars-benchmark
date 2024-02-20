.DEFAULT_GOAL := help

PYTHONPATH=
SHELL=/bin/bash
VENV=.venv
VENV_BIN=$(VENV)/bin

.venv:  ## Set up Python virtual environment and install requirements
	python3 -m venv $(VENV)
	$(MAKE) requirements

.PHONY: requirements
requirements: .venv  ## Update Python project requirements
	$(VENV_BIN)/python -m pip install --upgrade pip
	$(VENV_BIN)/pip install --upgrade -r requirements.txt

.PHONY: fmt
fmt:  ## Run autoformatting and linting
	$(VENV_BIN)/ruff check
	$(VENV_BIN)/ruff format

.PHONY: pre-commit
pre-commit: fmt  ## Run all code quality checks

.PHONY: tables-scale-1
tables-scale-1: .venv  ## Generate data tables
	$(MAKE) -C tpch-dbgen all
	cd tpch-dbgen && ./dbgen -vf -s 1 && cd ..
	mkdir -p "tables_scale_1"
	mv tpch-dbgen/*.tbl tables_scale_1/
	$(VENV_BIN)/python prepare_files.py 1

.PHONY: tables-scale-10
tables-scale-10: .venv  ## Generate bigger data tables
	$(MAKE) -C tpch-dbgen all
	cd tpch-dbgen && ./dbgen -vf -s 10 && cd ..
	mkdir -p "tables_scale_10"
	mv tpch-dbgen/*.tbl tables_scale_10/
	$(VENV_BIN)/python prepare_files.py 10

.PHONY: run-polars
run-polars: .venv  ## Run polars benchmarks
	$(VENV_BIN)/python -m polars_queries.executor

.PHONY: run-pandas
run-pandas: .venv  ## Run pandas benchmarks
	$(VENV_BIN)/python -m pandas_queries.executor

.PHONY: run-pyspark
run-pyspark: .venv  ## Run pyspark benchmarks
	$(VENV_BIN)/python -m spark_queries.executor

.PHONY: run-dask
run-dask: .venv  ## Run dask benchmarks
	$(VENV_BIN)/python -m dask_queries.executor

.PHONY: run-duckdb
run-duckdb: .venv  ## Run duckdb benchmarks
	$(VENV_BIN)/python -m duckdb_queries.executor

.PHONY: run-vaex
run-vaex: .venv  ## Run vaex benchmarks
	$(VENV_BIN)/python -m vaex_queries.executor

.PHONY: run-modin
run-modin: .venv  ## Run modin benchmarks
	$(VENV_BIN)/python -m modin_queries.executor

.PHONY: run-all
run-all: run-polars run-pandas run-pyspark run-dask run-duckdb run-vaex run-modin   ## Run all benchmarks

.PHONY: plot
plot: .venv  ## Plot results
	$(VENV_BIN)/python -m scripts.plot_results


.PHONY: clean
clean:  clean-tpch-dbgen clean-tables  ## Clean up everything
	@rm -rf .ruff_cache/
	@rm -rf .venv/

.PHONY: clean-tpch-dbgen
clean-tpch-dbgen:  ## Clean up TPC-H folder
	@$(MAKE) -C tpch-dbgen clean

.PHONY: clean-tables
clean-tables:  ## Clean up data tables
	@rm -rf tables_scale_*


.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort

