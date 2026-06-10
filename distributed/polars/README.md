# Polars Distributed Benchmark

## Prerequisites

A Polars Cloud license is required. Obtain one at https://cloud.pola.rs.

## Setup

Update the S3 bucket location in `values.yaml` to point to your TPC-H data.

## Running

```bash
helm upgrade --install polars polars-dev/polars --hide-notes -f values.yaml
```

## Hardware

Benchmark was run on 32x `m8id.xlarge` machines.
