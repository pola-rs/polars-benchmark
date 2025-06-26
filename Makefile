.DEFAULT_GOAL := help

PYTHONPATH=
SHELL=/bin/bash
VENV=.venv
VENV_BIN=$(VENV)/bin
NUM_PARTITIONS=10

.venv:  ## Set up Python virtual environment and install dependencies
	python3 -m venv $(VENV)
	$(MAKE) install-deps

.PHONY: install-deps
install-deps: .venv  ## Install Python project dependencies
	@unset CONDA_PREFIX \
	&& $(VENV_BIN)/python -m pip install --upgrade uv \
	&& $(VENV_BIN)/uv pip install --compile -r requirements.txt \
	&& $(VENV_BIN)/uv pip install --compile -r requirements-dev.txt

.PHONY: bump-deps
bump-deps: .venv  ## Bump Python project dependencies
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

ifndef SCALE_FACTOR

.PHONY: data-tables
data-tables:
	@echo "SCALE_FACTOR not set, skipping data table generation"

.PHONY: data-tables-partitioned
data-tables-partitioned:
	@echo "SCALE_FACTOR not set, skipping data table generation"

else

.PHONY: data-tables
data-tables: data/tables/scale-$(SCALE_FACTOR)

data/tables/scale-$(SCALE_FACTOR): .venv  ## Generate data tables
	# use tpch-cli
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	$(VENV_BIN)/tpchgen-cli --output-dir="data/tables/scale-$(SCALE_FACTOR)" --format=tbl -s $(SCALE_FACTOR)
	$(VENV_BIN)/python -m scripts.prepare_data --num-parts=1 --tpch_gen_folder="data/tables/scale-$(SCALE_FACTOR)"

	# use tpch-dbgen
	# $(MAKE) -C tpch-dbgen dbgen
	# cd tpch-dbgen && ./dbgen -vf -s $(SCALE_FACTOR) && cd ..
	# mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	# mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	# $(VENV_BIN)/python -m scripts.prepare_data --num-parts=1 --tpch_gen_folder="data/tables/scale-$(SCALE_FACTOR)"
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl

.PHONY: data-tables-partitioned
data-tables-partitioned: data/tables/scale-$(SCALE_FACTOR)/${NUM_PARTITIONS}

data/tables/scale-$(SCALE_FACTOR)/${NUM_PARTITIONS}: .venv  ## Generate partitioned data tables (these are not yet runnable with current repo)
	$(MAKE) -C tpch-dbgen dbgen
	$(VENV_BIN)/python -m scripts.prepare_data --num-parts=${NUM_PARTITIONS} --tpch_gen_folder="data/tables/scale-$(SCALE_FACTOR)"


endif

.PHONY: run-polars
run-polars: .venv data-tables  ## Run Polars benchmarks
	$(VENV_BIN)/python -m queries.polars

.PHONY: run-polars-no-env
run-polars-no-env: data-tables ## Run Polars benchmarks
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -f -s $(SCALE_FACTOR) && cd ..
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	python -m scripts.prepare_data
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	python -m queries.polars

.PHONY: run-polars-gpu-no-env
run-polars-gpu-no-env: run-polars-no-env data/tables/ ## Run Polars CPU and GPU benchmarks
	RUN_POLARS_GPU=true CUDA_MODULE_LOADING=EAGER python -m queries.polars

.PHONY: run-duckdb
run-duckdb: .venv data-tables ## Run DuckDB benchmarks
	$(VENV_BIN)/python -m queries.duckdb

.PHONY: run-pandas
run-pandas: .venv data-tables ## Run pandas benchmarks
	$(VENV_BIN)/python -m queries.pandas

.PHONY: run-pyspark
run-pyspark: .venv data-tables ## Run PySpark benchmarks
	$(VENV_BIN)/python -m queries.pyspark

.PHONY: run-dask
run-dask: .venv data-tables ## Run Dask benchmarks
	$(VENV_BIN)/python -m queries.dask

.PHONY: run-modin
run-modin: .venv data-tables ## Run Modin benchmarks
	$(VENV_BIN)/python -m queries.modin

.PHONY: run-all
run-all: run-polars run-duckdb run-pandas run-pyspark run-dask run-modin  ## Run all benchmarks

.PHONY: plot
plot: .venv  ## Plot results
	$(VENV_BIN)/python -m scripts.plot_bars

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
