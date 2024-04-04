.DEFAULT_GOAL := help

PYTHONPATH=
SHELL=/bin/bash
VENV=.venv
VENV_BIN=$(VENV)/bin

venv:  ## Set up Python virtual environment and install dependencies
	python3 -m venv $(VENV)
	$(MAKE) install-deps

.PHONY: install-deps
install-deps:  ## Install Python project dependencies
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip install -r requirements.txt
	$(VENV_BIN)/uv pip install -r requirements-dev.txt

.PHONY: bump-deps
bump-deps: venv  ## Bump Python project dependencies
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip compile requirements.in > requirements.txt
	$(VENV_BIN)/uv pip compile requirements-dev.in > requirements-dev.txt

.PHONY: fmt
fmt:  ## Run autoformatting and linting
	$(VENV_BIN)/ruff check
	$(VENV_BIN)/ruff format
	$(VENV_BIN)/mypy

.PHONY: pre-commit
pre-commit: fmt  ## Run all code quality checks

.PHONY: tables
tables: venv  ## Generate data tables
	$(MAKE) -C tpch-dbgen all
	cd tpch-dbgen && ./dbgen -vf -s $(SCALE_FACTOR) && cd ..
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	$(VENV_BIN)/python scripts/prepare_data.py $(SCALE_FACTOR)

.PHONY: run-polars
run-polars: venv  ## Run polars benchmarks
	$(VENV_BIN)/python -m queries.polars.executor

.PHONY: run-duckdb
run-duckdb: venv  ## Run duckdb benchmarks
	$(VENV_BIN)/python -m queries.duckdb.executor

.PHONY: run-pandas
run-pandas: venv  ## Run pandas benchmarks
	$(VENV_BIN)/python -m queries.pandas.executor

.PHONY: run-dask
run-dask: venv  ## Run dask benchmarks
	$(VENV_BIN)/python -m queries.dask.executor

.PHONY: run-pyspark
run-pyspark: venv  ## Run pyspark benchmarks
	$(VENV_BIN)/python -m queries.pyspark.executor

.PHONY: run-all
run-all: run-polars run-pandas run-pyspark run-duckdb   ## Run all benchmarks

.PHONY: plot
plot: venv  ## Plot results
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
	@rm -rf data/tables/


.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_0-9-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort

