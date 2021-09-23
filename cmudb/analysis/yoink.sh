#!/bin/bash

BENCHMARKS=("smallbank" "tatp" "tpcc" "ycsb")

for BENCHMARK in "${BENCHMARKS[@]}"
do
  scp dev6:~/postgres/${BENCHMARK}_docker_compose.out ./${BENCHMARK}_docker_compose_replica.out
  scp dev6:~/postgres/${BENCHMARK}_docker_monitoring.out ./${BENCHMARK}_docker_monitoring_replica.out
  scp dev6:~/postgres/${BENCHMARK}_docker_monitoring.txt ./${BENCHMARK}_docker_monitoring_replica.txt
  scp dev10:~/postgres/${BENCHMARK}_docker_compose.out ./${BENCHMARK}_docker_compose_primary.out
  scp dev10:~/postgres/${BENCHMARK}_docker_monitoring.out ./${BENCHMARK}_docker_monitoring_primary.out
  scp dev10:~/postgres/${BENCHMARK}_docker_monitoring.txt ./${BENCHMARK}_docker_monitoring_primary.txt
  scp dev10:~/postgres/${BENCHMARK}_replication_lag.out .
  scp dev10:~/postgres/${BENCHMARK}_replication_lag.txt .
  scp dev10:~/benchbase/target/benchbase-2021-SNAPSHOT/${BENCHMARK}_create.out .
  scp dev10:~/benchbase/target/benchbase-2021-SNAPSHOT/${BENCHMARK}_load.out .
  scp dev10:~/benchbase/target/benchbase-2021-SNAPSHOT/${BENCHMARK}_execute.out .
  scp dev10:~/config/benchbase/postgres/sample_${BENCHMARK}_config.xml .
done
