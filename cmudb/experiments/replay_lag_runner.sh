#!/bin/bash

set -e
set -x

sudo rm -rf /tmp/prometheus
sudo mkdir -p /tmp/prometheus
sudo chown 65534:65534 /tmp/prometheus

docker-compose -f ./cmudb/env/docker-compose-monitoring.yml up --force-recreate &
MONITORING_PID=$!

# TODO(WAN): Hacky sleep to wait for monitoring to come online.
sleep 10

# CPUS=( 6 3 1 0.75 0.5 0.25 0.2 0.15 0.1 0.05 0.01 )
# CPUS=( 0.1 0.09 0.08 0.07 0.06 0.05 0.04 0.03 0.02 0.025 0.01 0.005 )
CPUS=( 0.05 0.045 0.04 0.035 0.03 0.025 0.02 0.015 0.01 )
for cpu in "${CPUS[@]}"
do
  ./cmudb/experiments/replay_lag.sh $cpu 2>&1 > out$cpu.txt
done

kill $MONITORING_PID
