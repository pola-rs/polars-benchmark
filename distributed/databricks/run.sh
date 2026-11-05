#!/usr/bin/env bash
# Run the PDS-H Spark SQL benchmark on Databricks via the Databricks CLI.
# See README.md for prerequisites and examples.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERIES_SPEC="$SCRIPT_DIR/../spark/spark-benchmark-job.yaml"

PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
DATA_PATH=""
INSTANCE_PROFILE_ARN=""
CLUSTER_ID=""
NUM_WORKERS=32
NODE_TYPE="m8i.xlarge"
DRIVER_NODE_TYPE=""
SPARK_VERSION="18.x-scala2.13"
PHOTON=true
AVAILABILITY="ON_DEMAND"
QUERIES="1-22"
WORKSPACE_DIR=""
SPARK_CONF=()
DRY_RUN=false

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Data:
  --data-path PATH            TPC-H parquet root containing one directory per table (required)

Cluster (a new job cluster is created unless --cluster-id is given):
  --instance-profile-arn ARN  Instance profile with read access to --data-path
  --num-workers N             Worker count (default: $NUM_WORKERS)
  --node-type TYPE            Worker instance type (default: $NODE_TYPE)
  --driver-node-type TYPE     Driver instance type (default: same as --node-type)
  --spark-version VERSION     Databricks runtime (default: $SPARK_VERSION)
  --no-photon                 Disable Photon (default: enabled)
  --availability MODE         ON_DEMAND | SPOT | SPOT_WITH_FALLBACK (default: $AVAILABILITY)
  --spark-conf KEY=VALUE      Extra Spark config, repeatable
  --cluster-id ID             Run on an existing all-purpose cluster instead

Other:
  --profile NAME              Databricks CLI profile (default: \$DATABRICKS_CONFIG_PROFILE or DEFAULT)
  --queries LIST              Queries to run, e.g. 1-22 or 1,3,5-7 (default: $QUERIES)
  --workspace-dir PATH        Where to upload the scripts
                              (default: /Workspace/Users/<you>/polars-benchmark)
  --dry-run                   Print the run payload and exit
  -h, --help                  Show this help
EOF
}

die() { echo "error: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-path) DATA_PATH="$2"; shift 2 ;;
        --instance-profile-arn) INSTANCE_PROFILE_ARN="$2"; shift 2 ;;
        --cluster-id) CLUSTER_ID="$2"; shift 2 ;;
        --num-workers) NUM_WORKERS="$2"; shift 2 ;;
        --node-type) NODE_TYPE="$2"; shift 2 ;;
        --driver-node-type) DRIVER_NODE_TYPE="$2"; shift 2 ;;
        --spark-version) SPARK_VERSION="$2"; shift 2 ;;
        --no-photon) PHOTON=false; shift ;;
        --availability) AVAILABILITY="$2"; shift 2 ;;
        --spark-conf) SPARK_CONF+=("$2"); shift 2 ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --queries) QUERIES="$2"; shift 2 ;;
        --workspace-dir) WORKSPACE_DIR="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; die "unknown argument: $1" ;;
    esac
done

[[ -n "$DATA_PATH" ]] || die "--data-path is required"
[[ -n "$CLUSTER_ID" || -n "$INSTANCE_PROFILE_ARN" ]] \
    || die "--instance-profile-arn is required when creating a new cluster"
[[ "$QUERIES" =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]] || die "invalid --queries: $QUERIES"
command -v databricks >/dev/null || die "databricks CLI not found, see README.md"
command -v jq >/dev/null || die "jq not found"

dbx() { databricks --profile "$PROFILE" "$@"; }

if $DRY_RUN; then
    USER_NAME="<you>"
else
    USER_NAME="$(dbx current-user me --output json | jq -r .userName)"
fi
WORKSPACE_DIR="${WORKSPACE_DIR:-/Workspace/Users/$USER_NAME/polars-benchmark}"

spark_conf_json="$(
    for kv in ${SPARK_CONF[@]+"${SPARK_CONF[@]}"}; do
        [[ "$kv" == *=* ]] || die "--spark-conf expects KEY=VALUE, got: $kv"
        jq -n --arg k "${kv%%=*}" --arg v "${kv#*=}" '{($k): $v}'
    done | jq -s 'add // {}'
)"

if [[ -n "$CLUSTER_ID" ]]; then
    compute_json="$(jq -n --arg id "$CLUSTER_ID" '{existing_cluster_id: $id}')"
else
    compute_json="$(jq -n \
        --arg spark_version "$SPARK_VERSION" \
        --arg node_type "$NODE_TYPE" \
        --arg driver_node_type "${DRIVER_NODE_TYPE:-$NODE_TYPE}" \
        --argjson num_workers "$NUM_WORKERS" \
        --arg runtime_engine "$($PHOTON && echo PHOTON || echo STANDARD)" \
        --arg availability "$AVAILABILITY" \
        --arg instance_profile_arn "$INSTANCE_PROFILE_ARN" \
        --arg user "$USER_NAME" \
        --argjson spark_conf "$spark_conf_json" \
        '{new_cluster: {
            spark_version: $spark_version,
            node_type_id: $node_type,
            driver_node_type_id: $driver_node_type,
            num_workers: $num_workers,
            runtime_engine: $runtime_engine,
            data_security_mode: "SINGLE_USER",
            single_user_name: $user,
            spark_conf: $spark_conf,
            aws_attributes: {
                availability: $availability,
                first_on_demand: 1,
                zone_id: "auto",
                instance_profile_arn: $instance_profile_arn,
                ebs_volume_type: "GENERAL_PURPOSE_SSD",
                ebs_volume_count: 1,
                ebs_volume_size: 100
            },
            custom_tags: {project: "polars-benchmark"}
        }}')"
fi

run_name="pdsh-$(date -u +%Y%m%dT%H%M%SZ)"
payload="$(jq -n \
    --arg run_name "$run_name" \
    --arg python_file "$WORKSPACE_DIR/benchmark.py" \
    --arg queries_file "$WORKSPACE_DIR/spark-benchmark-job.yaml" \
    --arg data_path "$DATA_PATH" \
    --arg queries "$QUERIES" \
    --argjson compute "$compute_json" \
    '{
        run_name: $run_name,
        timeout_seconds: 14400,
        tasks: [({
            task_key: "pdsh",
            spark_python_task: {
                python_file: $python_file,
                parameters: [
                    "--data-path", $data_path,
                    "--queries-file", $queries_file,
                    "--queries", $queries
                ]
            }
        } + $compute)]
    }')"

if $DRY_RUN; then
    echo "$payload"
    exit 0
fi

echo "Uploading scripts to $WORKSPACE_DIR"
dbx workspace mkdirs "$WORKSPACE_DIR"
dbx workspace import "$WORKSPACE_DIR/benchmark.py" \
    --file "$SCRIPT_DIR/benchmark.py" --format AUTO --overwrite
dbx workspace import "$WORKSPACE_DIR/spark-benchmark-job.yaml" \
    --file "$QUERIES_SPEC" --format AUTO --overwrite

run_id="$(dbx jobs submit --no-wait --json "$payload" | jq -r .run_id)"
run="$(dbx jobs get-run "$run_id" --output json)"
echo "Submitted $run_name (run_id $run_id): $(jq -r .run_page_url <<<"$run")"

last=""
while true; do
    run="$(dbx jobs get-run "$run_id" --output json)"
    state="$(jq -r '.state.life_cycle_state' <<<"$run")"
    if [[ "$state" != "$last" ]]; then
        echo "$(date +%H:%M:%S) $state"
        last="$state"
    fi
    case "$state" in
        TERMINATED|SKIPPED|INTERNAL_ERROR) break ;;
    esac
    sleep 30
done

task_run_id="$(jq -r '.tasks[0].run_id' <<<"$run")"
output="$(dbx jobs get-run-output "$task_run_id" --output json)"

mkdir -p "$SCRIPT_DIR/results"
log_file="$SCRIPT_DIR/results/$run_name.log"
jq -r '.logs // ""' <<<"$output" >"$log_file"
cat "$log_file"
echo "Logs saved to $log_file"

result="$(jq -r '.state.result_state' <<<"$run")"
if [[ "$result" != "SUCCESS" ]]; then
    echo "Run finished with $result: $(jq -r '.state.state_message' <<<"$run")" >&2
    jq -r '.error // empty' <<<"$output" >&2
    exit 1
fi
