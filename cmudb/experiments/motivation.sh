#!/bin/bash

set -e

HOST_PRIMARY="dev10.db.pdl.local.cmu.edu"
USER_PRIMARY="wanshenl"
PGDB_PRIMARY="noisepage"
PGUSER_PRIMARY="noisepage"
PGPASS_PRIMARY="terrier"
PGPORT_PRIMARY="15721"
PGDIR_PRIMARY="/home/wanshenl/postgres"

HOST_REPLICA="dev6.db.pdl.local.cmu.edu"
USER_REPLICA="wanshenl"
PGDB_REPLICA="noisepage"
PGUSER_REPLICA="noisepage"
PGPASS_REPLICA="terrier"
PGPORT_REPLICA="15721"
PGDIR_REPLICA="/home/wanshenl/postgres"

HOST_BENCHBASE=${HOST_PRIMARY}
USER_BENCHBASE="wanshenl"
BINDIR_BENCHBASE="/home/wanshenl/benchbase/target/benchbase-2021-SNAPSHOT"
CFGDIR_BENCHBASE="/home/wanshenl/config/benchbase/postgres"

# Simple quick bash function rule: all vars used in a function must be declared at the start.

# Kill any leftover Docker instances.
function _kill_docker() {
  USER=$1
  HOST=$2

  ssh ${USER}@${HOST} "
    set -e
    docker kill \$(docker ps -q)
  "
}

function _murder_docker() {
  USER=$1
  HOST=$2

  ssh ${USER}@${HOST} "
    docker container prune
    docker volume prune
  "
}

# Bring up the primary.
function _primary_up() {
  FILENAME=$1

  USER=${USER_PRIMARY}
  HOST=${HOST_PRIMARY}
  PGDIR=${PGDIR_PRIMARY}

  ssh ${USER}@${HOST} "
    set -e
    cd ${PGDIR}
    docker-compose -f ./cmudb/env/docker-compose-primary.yml up --force-recreate > ${FILENAME}.out 2>&1 &
    echo \$! > ${FILENAME}.pid
  "
}

# Bring down the primary.
function _primary_down() {
  FILENAME=$1

  USER=${USER_PRIMARY}
  HOST=${HOST_PRIMARY}
  PGDIR=${PGDIR_PRIMARY}

  _kill_pid "${USER}" "${HOST}" "${PGDIR}" "${FILENAME}"

  ssh ${USER}@${HOST} "
    set -e
    cd ${PGDIR}
    docker-compose -f ./cmudb/env/docker-compose-primary.yml down
  "
}

# Bring up the replica.
function _replica_up() {
  FILENAME=$1

  USER=${USER_REPLICA}
  HOST=${HOST_REPLICA}
  PGDIR=${PGDIR_REPLICA}

  ssh ${USER}@${HOST} "
    set -e
    cd ${PGDIR}
    docker-compose -f ./cmudb/env/docker-compose-replica.yml up --force-recreate > ${FILENAME}.out 2>&1 &
    echo \$! > ${FILENAME}.pid
  "
}

# Bring down the replica.
function _replica_down() {
  FILENAME=$1

  USER=${USER_REPLICA}
  HOST=${HOST_REPLICA}
  PGDIR=${PGDIR_REPLICA}

  _kill_pid "${USER}" "${HOST}" "${PGDIR}" "${FILENAME}"

  ssh ${USER}@${HOST} "
    set -e
    cd ${PGDIR}
    docker-compose -f ./cmudb/env/docker-compose-replica.yml down
  "
}

# Set up docker monitoring.
function _setup_docker_monitoring() {
  USER=$1         # ssh user.
  HOST=$2         # ssh host.
  PGDIR=$3        # Directory of postgres.
  FILENAME=$4     # Name of file for .txt, .out, .pid.
  DOCKER_CONTAINER=$5  # Name of docker container to monitor.

  ssh ${USER}@${HOST} "
    set -e
    cd ${PGDIR}
    ./cmudb/experiments/capture_docker_stats.sh ${DOCKER_CONTAINER} ${FILENAME}.txt > ${FILENAME}.out 2>&1 &
    echo \$! > ${FILENAME}.pid
  "
}

# Set up replication monitoring.
# This is done on the primary because only the primary truly knows how far behind the replica is.
function _setup_replication_monitoring() {
  FILENAME=$1     # Name of file for .txt, .out, .pid.

  USER=${USER_PRIMARY}
  HOST=${HOST_PRIMARY}
  PGUSER=${PGUSER_PRIMARY}
  PGPASS=${PGPASS_PRIMARY}
  PGPORT=${PGPORT_PRIMARY}
  PGDIR=${PGDIR_PRIMARY}

  ssh ${USER}@${HOST} "
    set -e
    cd ${PGDIR}
    echo 'Timestamp|Replay Lag (microseconds)' > ${FILENAME}.txt
    while true
    do
      PGPASSWORD=${PGPASS} psql -h localhost -p ${PGPORT} -U ${PGUSER} -c 'select current_timestamp, extract(microseconds from replay_lag) from pg_stat_replication;' --tuples-only --no-align >> ${FILENAME}.txt
      sleep 1
    done > ${FILENAME}.out 2>&1 &
    echo \$! > ${FILENAME}.pid
  "
}

# Kill the PID in the specified file.
function _kill_pid() {
  USER=$1         # ssh user.
  HOST=$2         # ssh host.
  PGDIR=$3        # Directory of postgres.
  FILENAME=$4     # Name of file for .pid to kill and remove.

  ssh ${USER}@${HOST} "
    set -e
    cd ${PGDIR}
    kill \$(cat ${FILENAME}.pid)
    rm ${FILENAME}.pid
  "
}

# TODO(WAN): Docker's streaming API doesn't die when the parent dies.
function _force_kill_docker_metrics() {
  USER=$1         # ssh user.
  HOST=$2         # ssh host.

  ssh ${USER}@${HOST} "
    sudo kill -9 \$(ps aux | grep ${USER} | grep --invert-match aux | grep docker | grep nc | awk '{print \$2}')
  "
}

function _create_fn_warm_all() {
  USER=$1
  HOST=$2
  PGUSER=$3
  PGPASS=$4
  PGPORT=$5

  ssh ${USER}@${HOST} "
    set -e
    PGPASSWORD=${PGPASS} psql -h localhost -p ${PGPORT} -U ${PGUSER} -c 'drop extension if exists pg_prewarm;'
    PGPASSWORD=${PGPASS} psql -h localhost -p ${PGPORT} -U ${PGUSER} -c 'create extension pg_prewarm;'
    PGPASSWORD=${PGPASS} psql -h localhost -p ${PGPORT} -U ${PGUSER} -c \"create or replace function warm_all() returns table (name varchar, type varchar, pg_prewarm_val integer) language plpgsql as \\\$func\$ begin return query select cast(concat('public.', table_name) as varchar), 'table'::varchar, cast(pg_prewarm(concat('public.', table_name)) as integer) from information_schema.tables where table_schema = 'public' union all select cast(concat('public.', indexname) as varchar), 'index'::varchar, cast(pg_prewarm(concat('public.', indexname)) as integer) from pg_indexes where schemaname = 'public'; end \\\$func\$;\"
  "
}

# Run a PROPERLY QUOTED SQL query. This has a ton of footguns around quoting and escaping.
function _run_sql() {
  USER=$1
  HOST=$2
  PGUSER=$3
  PGPASS=$4
  PGPORT=$5
  SQL=$6

  ssh ${USER}@${HOST} "
    set -e
    PGPASSWORD=${PGPASS} psql -h localhost -p ${PGPORT} -U ${PGUSER} -c '${SQL}'
  "
}

# Run a PROPERLY QUOTED SQL query on the primary.
function _run_primary() {
  SQL=$1
  _run_sql "${USER_PRIMARY}" "${HOST_PRIMARY}" "${PGUSER_PRIMARY}" "${PGPASS_PRIMARY}" "${PGPORT_PRIMARY}" "${SQL}"
}

# Run a PROPERLY QUOTED SQL query on the replica.
function _run_replica() {
  SQL=$1
  _run_sql "${USER_REPLICA}" "${HOST_REPLICA}" "${PGUSER_REPLICA}" "${PGPASS_REPLICA}" "${PGPORT_REPLICA}" "${SQL}"
}

# Wait for the replica to be in sync.
function _replica_wait_sync() {
  USER=${USER_REPLICA}
  HOST=${HOST_REPLICA}
  PGUSER=${PGUSER_REPLICA}
  PGPASS=${PGPASS_REPLICA}
  PGPORT=${PGPORT_REPLICA}

  ssh ${USER}@${HOST} "
    set -e
    while true
    do
      SYNCED=\$(PGPASSWORD=${PGPASS} psql -h localhost -p ${PGPORT} -U ${PGUSER} -c 'select pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn();' --tuples-only --no-align)
      if [ "\$SYNCED" = "t" ]
      then
        break
      fi
      sleep 10
    done
  "
}

# Invoke benchbase.
function _benchbase() {
  BENCHMARK=$1
  FILENAME=$2
  COMMAND=$3

  USER=${USER_BENCHBASE}
  HOST=${HOST_BENCHBASE}
  BINDIR=${BINDIR_BENCHBASE}
  CFGDIR=${CFGDIR_BENCHBASE}

  ssh ${USER}@${HOST} "
    set -e
    cd ${BINDIR}
    java -jar ${BINDIR}/benchbase.jar -b ${BENCHMARK} -c ${CFGDIR}/sample_${BENCHMARK}_config.xml ${COMMAND} > ${FILENAME}.out 2>&1
  "
}

function _main() {
  echo "Primary: ${HOST_PRIMARY}, User: ${USER_PRIMARY}"
  echo "Replica: ${HOST_REPLICA}, User: ${USER_REPLICA}"
  echo "BenchBase: ${HOST_BENCHBASE}"

  BENCHMARKS=("smallbank" "tatp" "tpcc" "ycsb")

  set -x

  for BENCHMARK in ${BENCHMARKS[@]}
  do
    set +e
    _kill_docker "${USER_PRIMARY}" ${HOST_PRIMARY}
    _kill_docker "${USER_REPLICA}" ${HOST_REPLICA}
    _murder_docker "${USER_PRIMARY}" ${HOST_PRIMARY}
    _murder_docker "${USER_REPLICA}" ${HOST_REPLICA}
    set -e
    sleep 30

    # Bring up the primary and replica.
    _primary_up "${BENCHMARK}_docker_compose"
    _replica_up "${BENCHMARK}_docker_compose"

    set +x
    set +e
    while true
    do
      pg_isready -h ${HOST_PRIMARY} -p ${PGPORT_PRIMARY} -d ${PGDB_PRIMARY}
      PRIMARY_READY=$?
      if [ "${PRIMARY_READY}" == "0" ]
      then
        break
      fi
      sleep 10
    done
    while true
    do
      pg_isready -h ${HOST_REPLICA} -p ${PGPORT_REPLICA} -d ${PGDB_REPLICA}
      REPLICA_READY=$?
      if [ "${REPLICA_READY}" == "0" ]
      then
        break
      fi
      sleep 10
    done
    set -e
    set -x


    # Create the warm_all() function which invokes pg_prewarm on all tables and indexes.
    # This cannot be created on the replica because the replica is in read-only mode.
    _create_fn_warm_all "${USER_PRIMARY}" "${HOST_PRIMARY}" "${PGUSER_PRIMARY}" "${PGPASS_PRIMARY}" "${PGPORT_PRIMARY}"

    # Create and load the benchmark.
    _benchbase "${BENCHMARK}" "${BENCHMARK}_create" "--create=true"
    _benchbase "${BENCHMARK}" "${BENCHMARK}_load" "--load=true"

    # Wait for the replica to sync up data.
    _run_primary 'VACUUM FULL;'
    _replica_wait_sync

    # Warm the caches.
    _run_primary 'SELECT * FROM warm_all();'
    _replica_wait_sync

    # Collect Docker metrics on both primary and replica.
    _setup_docker_monitoring "${USER_PRIMARY}" "${HOST_PRIMARY}" "${PGDIR_PRIMARY}" "${BENCHMARK}_docker_monitoring" "primary"
    _setup_docker_monitoring "${USER_REPLICA}" "${HOST_REPLICA}" "${PGDIR_REPLICA}" "${BENCHMARK}_docker_monitoring" "replica"

    # Collect replication lag.
    _setup_replication_monitoring "${BENCHMARK}_replication_lag"

    # Execute the workload.
    _benchbase "${BENCHMARK}" "${BENCHMARK}_execute" "--execute=true"

    set +e
    # Stop collecting replication lag.
    _kill_pid "${USER_PRIMARY}" "${HOST_PRIMARY}" "${PGDIR_PRIMARY}" "${BENCHMARK}_replication_lag"
    # Stop collecting Docker metrics.
    _force_kill_docker_metrics "${USER_PRIMARY}" "${HOST_PRIMARY}"
    _force_kill_docker_metrics "${USER_REPLICA}" "${HOST_REPLICA}"
    _kill_pid "${USER_PRIMARY}" "${HOST_PRIMARY}" "${PGDIR_PRIMARY}" "${BENCHMARK}_docker_monitoring"
    _kill_pid "${USER_REPLICA}" "${HOST_REPLICA}" "${PGDIR_REPLICA}" "${BENCHMARK}_docker_monitoring"
    set -e

    # Bring down the primary and replica.
    _replica_down "${BENCHMARK}_docker_compose"
    _primary_down "${BENCHMARK}_docker_compose"
  done
}

_main
