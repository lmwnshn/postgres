#!/bin/bash

CPUS=$1

BENCHBASE_BIN_DIR="/home/cmu-db-nuc-wan/IdeaProjects/benchbase/target/benchbase-2021-SNAPSHOT"
BENCHBASE_CONFIG_DIR="/home/cmu-db-nuc-wan/git/scripts/benchbase/postgres"

cd $BENCHBASE_BIN_DIR
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --create=true
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --load=true
PGPASSWORD=terrier psql -h localhost -U noisepage -p 15721 -c "VACUUM FULL;"
sleep 10
echo "Run execute now!"
# java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --execute=true &
PGPASSWORD=terrier psql -h localhost -p 15721 -U noisepage -c 'vacuum full;'
cd -

sudo docker update replica-physical-test --cpus ${CPUS}

echo 'Timestamp|Replay Lag (microseconds)' > replay_lag_${CPUS}.txt
while true; do
  PGPASSWORD=terrier psql -h localhost -p 15721 -U noisepage -c 'select current_timestamp, extract(microseconds from replay_lag) from pg_stat_replication;' --tuples-only --no-align >> replay_lag.txt
  sleep 1
done
