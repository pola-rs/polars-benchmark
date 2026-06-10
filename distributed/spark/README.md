# Spark Distributed Benchmark

## Setup

Update the S3 bucket location in `spark-benchmark-job.yaml` to point to your TPC-H data.

## Running

Deploy the Spark cluster:

```bash
kubectl apply -f spark-cluster.yaml
```

Then run the benchmark job:

```bash
kubectl apply -f spark-benchmark-job.yaml
```

## Hardware

Benchmark was run on 32x `m8id.xlarge` machines.
