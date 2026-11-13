# Polars Distributed Benchmark

## Prerequisites

A Polars Cloud license is required. Obtain one at https://cloud.pola.rs.

## Setup

Update the data path in `pl_cloud.py` to point to your TPC-H data, and
`anonymousResults.s3.endpoint` in `values.yaml` to a bucket the workers can write query results to.

## Running

```bash
helm repo add polars-inc https://polars-inc.github.io/helm-charts && helm repo update
helm upgrade --install polars polars-inc/polars --version 3.2.0 --hide-notes -f values.yaml
```

## Hardware

Benchmark was run on 32x `m8i.xlarge` machines.
