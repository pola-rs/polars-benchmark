#!/usr/bin/env bash
set -euo pipefail

GO_VERSION=1.24.4
S5CMD_VERSION=2.3.0

if ! command -v s5cmd &>/dev/null; then
    if ! command -v go &>/dev/null; then
        echo "=== Installing Go ${GO_VERSION} ==="
        curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tar.gz
        sudo rm -rf /usr/local/go
        sudo tar -C /usr/local -xzf /tmp/go.tar.gz
        rm /tmp/go.tar.gz
        export PATH=$PATH:/usr/local/go/bin
    fi

    echo "=== Installing s5cmd ${S5CMD_VERSION} ==="
    sudo GOBIN=/usr/local/bin /usr/local/go/bin/go install "github.com/peak/s5cmd/v2@v${S5CMD_VERSION}"
fi

SCALE_FACTOR=${SCALE_FACTOR:-1000.0}
DEST=/data/tables/scale-${SCALE_FACTOR}/200
S3_PATH=s3://polars-pdsh-eu-central/scale-factor-${SCALE_FACTOR}/200/
TABLES=(customer lineitem nation orders part partsupp region supplier)

echo "=== Downloading TPC-H SF${SCALE_FACTOR} from S3 ==="
AWS_REGION=eu-central-1 s5cmd sync "${S3_PATH}*" "$DEST/"

echo "=== Restructuring files into subdirectories ==="
for table in "${TABLES[@]}"; do
    dir="$DEST/$table"
    shopt -s nullglob
    files=("$dir"/*.parquet)
    shopt -u nullglob
    if [[ ${#files[@]} -eq 0 ]]; then
        echo "  $table: skipping (already restructured)"
        continue
    fi
    echo "  $table: restructuring ${#files[@]} files"
    for f in "${files[@]}"; do
        stem=$(basename "$f" .parquet)
        mkdir -p "$dir/$stem"
        mv "$f" "$dir/$stem/part.parquet"
    done
done

echo "=== Done: $DEST ==="
