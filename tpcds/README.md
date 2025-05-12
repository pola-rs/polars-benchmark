# Polars Decision Support DS (PDS-DS) Benchmark

The official TPC-DS tools can be found at the website of TPC (https://www.tpc.org/tpcds/default5.asp). This repository is based on `DSGen-software-code-4.0.0`. Changes have been made to run this code on MACOS.

**NOTE**

This benchmark is currently in active development and not yet ready for benchmarking purposes.

---

## Build the required datagen tooling

```bash
>>> git clone https://github.com/pola-rs/polars-benchmark/
>>> cd polars-benchmark/tpcds/tools
```

### Run on Linux

```bash
>>> make dsdgen
```

### Run on MACOS

```bash
>>> make OS=MACOS dsdgen
```

## Generate TPC-DS datasets

```bash
# create a folder for data and set scaling factor with -scale

>>> mkdir -p ../data/
>>> ./dsdgen -scale 1 -dir ../data/
```
