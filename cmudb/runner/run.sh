#!/usr/bin/env bash

export ROOT_DIR=$(pwd)
export HOSTNAME=$(hostname)

export ARTIFACT_ROOT="${ROOT_DIR}/artifact"

if [ "${HOSTNAME}" = "dev8" ]; then
  export POSTGRES_BUILD_DIR="${ROOT_DIR}/build/postgres/"
  export POSTGRES_BIN_DIR="${ROOT_DIR}/build/postgres/bin/"
  export POSTGRES_DATA_DIR="/mnt/nvme1n1/postgres/data/"
else
  export POSTGRES_BUILD_DIR="${ROOT_DIR}/build/postgres/"
  export POSTGRES_BIN_DIR="${ROOT_DIR}/build/postgres/bin/"
  export POSTGRES_DATA_DIR="${ROOT_DIR}/data/"
fi

export POSTGRES_USER="noisepage_user"
export POSTGRES_PASS="noisepage_pass"
export POSTGRES_DB="noisepage_db"
export POSTGRES_HOST="localhost"
export POSTGRES_PORT=15799
export POSTGRES_PID=-1

export TPCH_REPO_ROOT="${ROOT_DIR}/build/tpch-kit/"
export TPCH_DATA_ROOT="${ROOT_DIR}/build/tpch-data/"
export TPCH_SCHEMA_ROOT="${ROOT_DIR}/build/tpch-schema/"
export TPCH_QUERY_ROOT="${ROOT_DIR}/build/tpch-queries/"
export TPCH_QUERY_START=15721
export TPCH_QUERY_STOP=16720

export DSB_REPO_ROOT="${ROOT_DIR}/build/dsb/"
export DSB_DATA_ROOT="${ROOT_DIR}/build/dsb-data/"
export DSB_SCHEMA_ROOT="${ROOT_DIR}/build/dsb-schema/"
export DSB_QUERY_ROOT="${ROOT_DIR}/build/dsb-queries/"
export DSB_QUERY_TRAIN_SEED=15721
export DSB_QUERY_TRAIN_NUM=200
export DSB_QUERY_TEST_SEED=15722
export DSB_QUERY_TEST_NUM=200

export JOB_REPO_ROOT="${ROOT_DIR}/build/job/"
export JOB_DATA_ROOT="${ROOT_DIR}/build/job/raw_data/"
export JOB_SCHEMA_ROOT="${ROOT_DIR}/build/job/queries/"
export JOB_QUERY_ROOT="${ROOT_DIR}/build/job/queries/"

main() {
  kill "$(lsof -t -i:15799)"
  set -euxo pipefail

  setup_postgres

  setup_tpch
  setup_dsb
  setup_job

  load_tpch_sf 1
  run_tpch_sf 1
  mv ./artifact ./artifact_tpch

  load_dsb_sf 1
  run_dsb_sf 1
  mv ./artifact ./artifact_dsb

  load_job
  run_job
  mv ./artifact ./artifact_job

  kill ${POSTGRES_PID}
}

sql_create_database() {
  set +e
  "${POSTGRES_BIN_DIR}/psql" -c "create database ${POSTGRES_DB} with owner = '${POSTGRES_USER}'" postgres -p "${POSTGRES_PORT}"
  set -e
}

sql_database_exists() {
  local EXISTS=$("${POSTGRES_BIN_DIR}/psql" -d template1 -p "${POSTGRES_PORT}" --tuples-only --no-align -c "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'")
  if [ "${EXISTS}" = "1" ]; then
    return 0
  else
    return 1
  fi
}

setup_postgres() {
  cd "${ROOT_DIR}"
  mkdir -p "${POSTGRES_BUILD_DIR}"
  if [ ! -f "${ROOT_DIR}/config.log" ] || [ "${POSTGRES_RUN_CONFIGURE}" = "true" ]; then
    set +e
    make clean
    set -e
    ./cmudb/build/configure.sh "${POSTGRES_BUILD_TYPE}" "${POSTGRES_BUILD_DIR}"
  fi
  if [ "${POSTGRES_RUN_CLEAN}" = "true" ]; then
    make clean
  fi
  make install-world-bin -j

  if [ "${POSTGRES_RUN_CLEAR_DATA}" = "true" ]; then
    rm -rf "${POSTGRES_DATA_DIR}/pgdata"
  fi

  local RAN_INITDB="false"
  if [ ! -d "${POSTGRES_DATA_DIR}/pgdata" ]; then
    "${POSTGRES_BIN_DIR}/initdb" -D "${POSTGRES_DATA_DIR}/pgdata"
    RAN_INITDB="true"
  fi
  if [ "${HOSTNAME}" = "dev8" ]; then
    cp ./cmudb/env/dev8.pgtune.auto.conf "${POSTGRES_DATA_DIR}/pgdata/postgresql.auto.conf"
  else
    cp ./cmudb/env/default.pgtune.auto.conf "${POSTGRES_DATA_DIR}/pgdata/postgresql.auto.conf"
  fi

  "${POSTGRES_BIN_DIR}/postgres" -D "${POSTGRES_DATA_DIR}/pgdata" -p "${POSTGRES_PORT}" &
  POSTGRES_PID=$!

  until "${POSTGRES_BIN_DIR}/pg_isready" -p ${POSTGRES_PORT} &> /dev/null
  do
    sleep 1
  done

  if [ "${RAN_INITDB}" = "true" ]; then
    "${POSTGRES_BIN_DIR}/psql" -c "create user ${POSTGRES_USER} with login superuser password '${POSTGRES_PASS}'" postgres -p "${POSTGRES_PORT}"
    sql_create_database
  fi

  echo "Started Postgres, PID: ${POSTGRES_PID}"
}

setup_tpch() {
  ./cmudb/setup/tpch/tpch_setup.sh
  ./cmudb/setup/tpch/tpch_schema.sh
  ./cmudb/setup/tpch/tpch_queries.sh
}

load_tpch_sf() {
  local SF=$1
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="tpch_sf_${SF}"
  if ! sql_database_exists; then
    TPCH_SF="${SF}" ./cmudb/setup/tpch/tpch_data.sh
    sql_create_database
    TPCH_SF="${SF}" python3 ./cmudb/runner/tpch_load.py
    POSTGRES_DB="${OLD_POSTGRES_DB}"
  fi
}

run_tpch_sf() {
  local SF=$1
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="tpch_sf_${SF}"
  TPCH_SF="${SF}" python3 ./cmudb/runner/tpch_run.py
  POSTGRES_DB="${OLD_POSTGRES_DB}"
}

setup_dsb() {
  ./cmudb/setup/dsb/dsb_setup.sh
  ./cmudb/setup/dsb/dsb_schema.sh
  ./cmudb/setup/dsb/dsb_queries.sh
}

load_dsb_sf() {
  local SF=$1
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="dsb_sf_${SF}"
  if ! sql_database_exists; then
    DSB_SF="${SF}" ./cmudb/setup/dsb/dsb_data.sh
    sql_create_database
    DSB_SF="${SF}" python3 ./cmudb/runner/dsb_load.py
    POSTGRES_DB="${OLD_POSTGRES_DB}"
  fi
}

run_dsb_sf() {
  local SF=$1
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="dsb_sf_${SF}"
  DSB_SF="${SF}" python3 ./cmudb/runner/dsb_run.py
  POSTGRES_DB="${OLD_POSTGRES_DB}"
}

setup_job() {
  ./cmudb/setup/job/job_setup.sh
}

load_job() {
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="job"
  if ! sql_database_exists; then
    sql_create_database
    python3 ./cmudb/runner/job_load.py
    POSTGRES_DB="${OLD_POSTGRES_DB}"
  fi
}

run_job() {
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="job"
  python3 ./cmudb/runner/job_run.py
  POSTGRES_DB="${OLD_POSTGRES_DB}"
}

main "$@"