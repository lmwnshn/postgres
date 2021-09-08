#!/bin/bash

BENCHBASE_BIN_DIR="/home/cmu-db-nuc-wan/IdeaProjects/benchbase/target/benchbase-2021-SNAPSHOT"
BENCHBASE_CONFIG_DIR="/home/cmu-db-nuc-wan/git/scripts/benchbase/postgres"

cd $BENCHBASE_BIN_DIR
# java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --create=true
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --load=true
PGPASSWORD=terrier psql -h localhost -U noisepage -p 15721 -c "VACUUM FULL;"
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --execute=true
PGPASSWORD=terrier psql -h localhost -U noisepage -p 15721 -c "VACUUM FULL;"
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --execute=true
PGPASSWORD=terrier psql -h localhost -U noisepage -p 15721 -c "VACUUM FULL;"
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --execute=true
PGPASSWORD=terrier psql -h localhost -U noisepage -p 15721 -c "VACUUM FULL;"
java -jar $BENCHBASE_BIN_DIR/benchbase.jar -b tpcc -c $BENCHBASE_CONFIG_DIR/sample_tpcc_config.xml --execute=true
cd -
