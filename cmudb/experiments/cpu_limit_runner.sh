#!/bin/bash

set -e
set -x

CPUS=( 0.5 0.25 0.2 0.15 0.1 0.09 0.08 0.07 0.06 0.05 0.04 0.03 0.02 0.01 )
for cpu in "${CPUS[@]}"
do
  ./cmudb/experiments/cpu_limit.sh $cpu 2>&1 > ${cpu}_out.txt
done
