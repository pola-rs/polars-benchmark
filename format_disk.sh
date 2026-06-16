#!/usr/bin/env bash
set -euo pipefail

echo "=== Installing mdadm ==="
sudo yum install -y mdadm

echo "=== Creating RAID0 across 4 NVMe SSDs ==="
sudo mdadm --create /dev/md0 --level=0 --raid-devices=2 \
  /dev/nvme1n1 /dev/nvme2n1 \
  --run --force

echo "=== Formatting with xfs ==="
sudo mkfs.xfs /dev/md0

echo "=== Mounting at /data ==="
sudo mkdir -p /data
sudo mount /dev/md0 /data
sudo chown -R ec2-user:ec2-user /data
sudo df -h /data
