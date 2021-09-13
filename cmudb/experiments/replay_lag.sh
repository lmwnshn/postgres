#!/bin/bash

CPUS=$1

set -e
set -x

BENCHBASE_BIN_DIR="/home/cmu-db-nuc-wan/IdeaProjects/benchbase/target/benchbase-2021-SNAPSHOT"
BENCHBASE_CONFIG_DIR="/home/cmu-db-nuc-wan/git/scripts/benchbase/postgres"

docker-compose -f ./cmudb/env/docker-compose-replication.yml up --force-recreate &
POSTGRES_PID=$!

sleep 45

cd $BENCHBASE_BIN_DIR
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --create=true
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --load=true
PGPASSWORD=terrier psql -h localhost -U noisepage -p 15721 -c "VACUUM FULL;"

while true ; do
  SYNCED=$(PGPASSWORD=terrier psql -h localhost -p 15722 -U noisepage -c 'select pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn();' --tuples-only --no-align)
  if [ "$SYNCED" = "t" ]; then
    break
  fi
  sleep 10
done

java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --execute=true &
EXECUTE_PID=$!
cd -

docker update replica-physical --cpus ${CPUS}

echo 'Timestamp|Replay Lag (microseconds)' > replay_lag_${CPUS}.txt

set +x
for i in {0..60..1}
do
  PGPASSWORD=terrier psql -h localhost -p 15721 -U noisepage -c 'select current_timestamp, extract(microseconds from replay_lag) from pg_stat_replication;' --tuples-only --no-align >> replay_lag_${CPUS}.txt
  sleep 1
done
set -x


# Cleanup.

kill $EXECUTE_PID
kill $POSTGRES_PID

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
