.DEFAULT_GOAL := help

SHELL=/bin/bash
VENV=.venv
VENV_BIN=$(VENV)/bin
PYTHON?=$(VENV_BIN)/python

.venv:  ## Set up Python virtual environment and install dependencies
	python3 -m venv $(VENV)
	$(MAKE) install-deps

.PHONY: install-deps
install-deps: .venv  ## Install Python project dependencies
	@unset CONDA_PREFIX \
	&& $(PYTHON) -m pip install --upgrade uv \
	&& $(VENV_BIN)/uv pip install --compile -r requirements.txt \
	&& $(VENV_BIN)/uv pip install --compile -r requirements-dev.txt

.PHONY: bump-deps
bump-deps: .venv  ## Bump Python project dependencies
	$(PYTHON) -m pip install --upgrade uv
	$(VENV_BIN)/uv pip compile requirements.in > requirements.txt
	$(VENV_BIN)/uv pip compile requirements-dev.in > requirements-dev.txt

.PHONY: fmt
fmt:  ## Run autoformatting and linting
	$(VENV_BIN)/ruff check
	$(VENV_BIN)/ruff format
	$(VENV_BIN)/mypy

.PHONY: pre-commit
pre-commit: fmt  ## Run all code quality checks

ifndef SCALE_FACTOR

data/tables/.generated:
	@echo "SCALE_FACTOR not set, skipping data table generation"
	@touch $@

data/tables/:
	@echo "SCALE_FACTOR not set, skipping data table generation"
	@mkdir -p $@

data/tables/partitioned/:
	@echo "SCALE_FACTOR not set, skipping data table generation"
	@mkdir -p $@

else

data/tables/.generated: .venv  ## Generate data tables
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -vf -s $(SCALE_FACTOR) && cd ..
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	$(PYTHON) -m scripts.prepare_data --num-parts=1 --tpch_gen_folder="data/tables/scale-$(SCALE_FACTOR)"
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	touch $@

data/tables/: data/tables/.generated
	@true

data/tables/partitioned/: .venv  ## Generate partitioned data tables (these are not yet runnable with current repo)
	$(MAKE) -C tpch-dbgen dbgen
	$(PYTHON) -m scripts.prepare_data --num-parts=10 --tpch_gen_folder="data/tables/scale-$(SCALE_FACTOR)"

endif

.PHONY: run-polars
run-polars: .venv data/tables/  ## Run Polars benchmarks
	$(PYTHON) -m queries.polars

# All the run-* targets other than run-polars assume that the data/tables have
# already been generated.
.PHONY: run-polars-no-env
run-polars-no-env:  ## Run Polars benchmarks
	python -m queries.polars

.PHONY: run-polars-gpu-no-env
run-polars-gpu-no-env: run-polars-no-env ## Run Polars CPU and GPU benchmarks
	RUN_POLARS_GPU=true CUDA_MODULE_LOADING=EAGER python -m queries.polars

.PHONY: run-duckdb
run-duckdb: .venv ## Run DuckDB benchmarks
	$(PYTHON) -m queries.duckdb

.PHONY: run-duckdb-no-env
run-duckdb-no-env: ## Run DuckDB benchmarks
	python -m queries.duckdb

.PHONY: run-pandas
run-pandas: .venv ## Run pandas benchmarks
	$(PYTHON) -m queries.pandas

.PHONY: run-pandas-no-env
run-pandas-no-env: ## Run pandas benchmarks
	python -m queries.pandas

.PHONY: run-pyspark
run-pyspark: .venv ## Run PySpark benchmarks
	$(PYTHON) -m queries.pyspark

.PHONY: run-pyspark-no-env
run-pyspark-no-env: ## Run PySpark benchmarks
	python -m queries.pyspark

.PHONY: run-dask
run-dask: .venv ## Run Dask benchmarks
	$(PYTHON) -m queries.dask

.PHONY: run-dask-no-env
run-dask-no-env: ## Run Dask benchmarks
	python -m queries.dask

.PHONY: run-modin
run-modin: .venv ## Run Modin benchmarks
	$(PYTHON) -m queries.modin

.PHONY: run-modin-no-env
run-modin-no-env: ## Run Modin benchmarks
	python -m queries.modin

.PHONY: run-all
run-all: run-polars run-duckdb run-pandas run-pyspark run-dask run-modin  ## Run all benchmarks

.PHONY: plot
plot: .venv  ## Plot results
	$(PYTHON) -m scripts.plot_bars

.PHONY: clean
clean:  clean-tpch-dbgen clean-tables  ## Clean up everything
	$(VENV_BIN)/ruff clean
	@rm -rf .mypy_cache/
	@rm -rf .venv/
	@rm -rf output/
	@rm -rf spark-warehouse/

.PHONY: clean-tpch-dbgen
clean-tpch-dbgen:  ## Clean up TPC-H folder
	@$(MAKE) -C tpch-dbgen clean
	@rm -rf tpch-dbgen/*.tbl

.PHONY: clean-tables
clean-tables:  ## Clean up data tables
	@rm -rf data/tables/

.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_0-9-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort
