# Polars Decision Support (PDS) benchmarks

## Disclaimer

Polars Decision Support (PDS) benchmarks are derived from the TPC-H Benchmarks and as such any results obtained using PDS are not comparable to published TPC-H Benchmark results, as the results obtained from using PDS do not comply with the TPC-H Benchmarks.

These benchmarks are our adaptation of an industry-standard decision support benchmark often used in the DataFrame library community. PDS consists of the same 22 queries as the industry standard benchmark TPC-H, but has modified parts for dataset generation and execution scripts.

From the [TPC website](https://www.tpc.org/tpch/):
> TPC-H is a decision support benchmark. It consists of a suite of business-oriented ad hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance. This benchmark illustrates decision support systems that examine large volumes of data, execute queries with a high degree of complexity, and give answers to critical business questions.

## License

PDS is licensed under Apache License, Version 2.0.

Additionally, certain files in PDS are licensed subject to the accompanying [TPC EULA](TPC%20EULA.txt) (also available at <http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp>). Files subject to the TPC EULA are identified as such within the files.

You may not use PDS except in compliance with the Apache License, Version 2.0 and the TPC EULA.

## Generating PDS Benchmarking Data

### Project setup

```shell
# clone this repository
git clone https://github.com/pola-rs/polars-benchmark.git
cd polars-benchmark/tpch-dbgen

# build tpch-dbgen
make
```

### Execute

```shell
# change directory to the root of the repository
cd ../
./run.sh
```

This will do the following,

- Create a new virtual environment with all required dependencies.
- Generate data for benchmarks.
- Run the benchmark suite.

> **Note:** To run the Exasol benchmarks, ensure your Exasol database is configured via environment variables (or a `.env` file) with the following settings:
>
> ```
> EXASOL_HOST=<host>
> EXASOL_USER=<user>
> EXASOL_PASSWORD=<password>
> (optional) EXASOL_PORT=<port>          # default: 8563
> (optional) EXASOL_SCHEMA_NAME=<schema> # default: tpc
> ```
>
> You can set SCALE_FACTOR=<scale> (default: 1.0) to control the data scale.
> Then execute:
>
> ```shell
> make run-exasol
> ```
>
> The Exasol runner executes the DDL scripts in three phases:
>
> 1. `create_schema.sql` to create empty TPC-H tables.
> 2. Import data files into these tables from the local filesystem using Exasol's
>    `IMPORT FROM` functionality (via `pyexasol.import_from_file`).
> 3. Run `create_indices_1node.sql` and `analyze_database.sql` to enforce indices
>    and collect statistics.
>
> Finally, it executes the 22 TPC-H queries.
