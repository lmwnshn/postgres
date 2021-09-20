#!/bin/bash

CPUS=$1

EXPECTED_BENCHBASE_DURATION=180

set -e
set -x

HOME_DIR="/home/wanshenl"
BENCHBASE_BIN_DIR="$HOME_DIR/benchbase/target/benchbase-2021-SNAPSHOT"
BENCHBASE_CONFIG_DIR="$HOME_DIR/config/benchbase/postgres"

#docker-compose -f ./cmudb/env/docker-compose-replication.yml up --force-recreate &
#POSTGRES_PID=$!

sleep 45

cd $BENCHBASE_BIN_DIR
#java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b ycsb -c $BENCHBASE_CONFIG_DIR/sample_ycsb_config.xml --create=true
#java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b ycsb -c $BENCHBASE_CONFIG_DIR/sample_ycsb_config.xml --load=true
PGPASSWORD=terrier psql -h localhost -U noisepage -p 15721 -c "VACUUM FULL;"
cd -

while true ; do
  SYNCED=$(PGPASSWORD=terrier psql -h localhost -p 15722 -U noisepage -c 'select pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn();' --tuples-only --no-align)
  if [ "$SYNCED" = "t" ]; then
    break
  fi
  sleep 10
done

./cmudb/experiments/capture_docker_stats.sh primary cpu_${CPUS}_docker_stats_primary.txt &
MONITORING_PID_PRIMARY=$!
./cmudb/experiments/capture_docker_stats.sh replica cpu_${CPUS}_docker_stats_replica.txt &
MONITORING_PID_REPLICA=$!

docker update replica --cpus ${CPUS}

cd $BENCHBASE_BIN_DIR
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b ycsb -c $BENCHBASE_CONFIG_DIR/sample_ycsb_config.xml --execute=true 2>&1 > cpu_${CPUS}_ycsb.txt &
EXECUTE_PID=$!
cd -

echo 'Timestamp|Replay Lag (microseconds)' > cpu_${CPUS}_replay_lag.txt

set +x
i = 1
while [ "$i" -le "$EXPECTED_BENCHBASE_DURATION" ]; do
  PGPASSWORD=terrier psql -h localhost -p 15721 -U noisepage -c 'select current_timestamp, extract(microseconds from replay_lag) from pg_stat_replication;' --tuples-only --no-align >> cpu_${CPUS}_replay_lag.txt
  sleep 1
  i=$(($i + 1))
done
set -x

# Cleanup.

wait $EXECUTE_PID
kill $MONITORING_PID_PRIMARY
kill $MONITORING_PID_REPLICA
#kill $POSTGRES_PID

kill_descendant_processes() {
    local pid="$1"
    local and_self="${2:-false}"
    if children="$(pgrep -P "$pid")"; then
        for child in $children; do
            kill_descendant_processes "$child" true
        done
    fi
    if [[ "$and_self" == true ]]; then
        sudo kill -9 "$pid"
    fi
}
kill_descendant_processes $$
