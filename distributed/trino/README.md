# Trino Distributed Benchmark

## Setup

Update the S3 bucket location in `trino-benchmark-job.yaml` to point to your TPC-H data, and the S3 region in `values.yaml` and `trino-metastore.yaml` to match it.

## Running

Deploy the Hive metastore config and the Trino cluster:

```bash
kubectl apply -f trino-metastore.yaml
helm repo add trino https://trinodb.github.io/charts
helm upgrade --install trino trino/trino --version 1.45.0 -f values.yaml
```

Once all workers are ready, run the benchmark job:

```bash
kubectl apply -f trino-benchmark-job.yaml
```

## Hardware

Benchmark was run on 33x `m8i.xlarge` machines (1 coordinator, 32 workers).
