#!/bin/bash

set -e

CPUS=( 0.5 0.25 0.2 0.15 0.1 0.09 0.08 0.07 0.06 0.05 0.04 0.03 0.02 0.01 )

# Quick sudo command so that resulting scripts have it.
sudo echo "CPUS: ${CPUS[@]}"
set -x

# docker build --tag pgnp --file ./cmudb/env/Dockerfile .
for cpu in "${CPUS[@]}"
do
  ./cmudb/experiments/cpu_limit.sh $cpu 2>&1 > cpu_${cpu}_out.txt
done
