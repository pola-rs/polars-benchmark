# Databricks Distributed Benchmark

Runs the Spark SQL queries from [`../spark/spark-benchmark-job.yaml`](../spark/spark-benchmark-job.yaml)
on Databricks as a one-time job run, on a new job cluster or an existing all-purpose cluster.

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) v0.205+ and `jq`
- An authenticated CLI profile: `databricks auth login --host https://<workspace>.cloud.databricks.com`
- An instance profile registered in the workspace with read access to the TPC-H data

## Running

```bash
./run.sh --data-path s3://<bucket>/<tpch-root> \
    --instance-profile-arn arn:aws:iam::<account>:instance-profile/<name>
```

`run.sh` uploads `benchmark.py` and the query definitions to your workspace, submits the run,
waits for it to finish, and saves the driver output to `results/<run-name>.log`.

Useful options (`./run.sh --help` for all):

| Option                    | Default          |
| ------------------------- | ---------------- |
| `--profile`               | `DEFAULT`        |
| `--num-workers`           | `32`             |
| `--node-type`             | `m8i.xlarge`     |
| `--spark-version`         | `18.x-scala2.13` |
| `--no-photon`             | Photon enabled   |
| `--availability`          | `ON_DEMAND`      |
| `--queries`               | `1-22`           |
| `--spark-conf KEY=VALUE`  | none             |
| `--cluster-id`            | new job cluster  |
| `--dry-run`               | off              |

## Hardware

Benchmark was run on 32x `m8i.xlarge` workers plus one `m8i.xlarge` driver,
Databricks Runtime 18 LTS (Spark 4.1.0) with Photon.
