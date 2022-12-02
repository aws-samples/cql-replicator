#!/usr/bin/env bash
#
# // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# // SPDX-License-Identifier: Apache-2.0
#

MIGRATOR_VERSION=0.1alfa
SPARK_CASSANDRA_CONNECTOR=spark-cassandra-connector_2.11:2.5.2
WORKING_DIR=/Users/kolesnn/Work/CQLReplicator/spark
CONF_DIR=$WORKING_DIR/conf
SPARK_HOME=/Users/kolesnn/Downloads/spark-2.4.8-bin-hadoop2.7/bin
CASSANDRA_READ_THROUGHPUT=10
MAX_FAILURES=256
DRIVER_MEMORY=8g
EXECUTOR_MEMORY=2g
export SPARK_LOCAL_DIRS=$WORKING_DIR/tmp
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_231.jdk/Contents/Home

function Usage_Exit {
  echo "$0 [--batch-keyspace keyspace_name|--increment-keyspace keyspace_name|--validate-keyspace keyspace_name|--reconcile-keyspace keyspace_name]"
  echo "Script version:"${MIGRATOR_VERSION}
  exit
}

function Batch_Keyspace_Offload {
    tt=$(cat $CONF_DIR/config.json | jq '.'$KEYSPACE_NAME)
        for row in $(echo "${tt}" | jq -r '.[] | @base64');
                do
                    _jq() {
                        echo ${row} | base64 --decode | jq -r ${1}
                      }
                    table=$(_jq '.table_name')
                    timestamp_column=$(_jq '.timestamp_column')
                    replace_nulls=$(_jq '.replace_nulls')
                    sync_type=$(_jq '.sync_type')
                    if [[ "$sync_type" == "batch" ]]; then
                      echo "Processing table:"$table
                      $SPARK_HOME/spark-shell -i ${WORKING_DIR}/sbin/batch_table_offloader.scala \
                              --packages com.datastax.spark:${SPARK_CASSANDRA_CONNECTOR} \
                              --files $CONF_DIR/CassandraConnector.conf \
                              --conf spark.cassandra.connection.config.profile.path="CassandraConnector.conf" \
                              --conf spark.driver.args="${table} ${KEYSPACE_NAME} ${timestamp_column} ${WORKING_DIR} ${replace_nulls}"
                    fi
                done
}

function Batch_Keyspace_Load {
  for path_to_table in ${WORKING_DIR}/keyspaces/$KEYSPACE_NAME/*;
                do
                        echo "Processing table from "${path_to_table}
                        table_name=${path_to_table##*/}
                        concurrent_writes=$(cat $CONF_DIR/config.json | jq '.'${KEYSPACE_NAME}'[] | select(.table_name=="'$table_name'").concurrent_writes' | \
                         tr -d '"')
                        $SPARK_HOME/spark-shell -i ${WORKING_DIR}/sbin/batch_table_loader.scala \
                                --packages com.datastax.spark:${SPARK_CASSANDRA_CONNECTOR} \
                                --files $CONF_DIR/KeyspacesConnector.conf --conf spark.driver.memory=${DRIVER_MEMORY} \
                                --conf spark.executor.memory=${EXECUTOR_MEMORY} --conf spark.cassandra.connection.config.profile.path="KeyspacesConnector.conf" \
                                --conf spark.cassandra.output.concurrent.writes=$concurrent_writes --conf spark.cassandra.output.batch.size.rows=1 \
                                --conf spark.cassandra.output.batch.grouping.key=none --conf spark.task.maxFailures=${MAX_FAILURES} \
                                --conf spark.driver.args="${table_name} ${KEYSPACE_NAME} ${path_to_table} ${WORKING_DIR}"

                done

}

function Incremental_Keyspace_Offload {
  tt=$(cat $CONF_DIR/config.json | jq '.'$KEYSPACE_NAME)
                for row in $(echo "${tt}" | jq -r '.[] | @base64');
                        do
                            _jq() {
                            echo ${row} | base64 --decode | jq -r ${1}
                        }
                    table=$(_jq '.table_name')
                    timestamp_column=$(_jq '.timestamp_column')
                    sync_type=$(_jq '.sync_type')
                    replace_nulls=$(_jq '.replace_nulls')
                    echo "Processing table:"$table
                    if [[ "$sync_type" == "incremental" ]]; then
                      $SPARK_HOME/spark-shell -i ${WORKING_DIR}/sbin/incremental_table_offloader.scala \
                                  --packages com.datastax.spark:${SPARK_CASSANDRA_CONNECTOR} \
                                  --files $CONF_DIR/CassandraConnector.conf \
                                  --conf spark.driver.memory=${DRIVER_MEMORY} --conf spark.executor.memory=${EXECUTOR_MEMORY} \
                                  --conf spark.cassandra.connection.config.profile.path="CassandraConnector.conf" \
                                  --conf spark.cassandra.input.throughputMBPerSec=$CASSANDRA_READ_THROUGHPUT \
                                  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
                                  --conf spark.driver.args="$table ${KEYSPACE_NAME} ${timestamp_column} ${WORKING_DIR} $replace_nulls"
                    fi
                done
}

function Incremental_Keyspace_Load {
  for path_to_table in ${WORKING_DIR}/keyspaces/incremental/$KEYSPACE_NAME/*;
                do
                        echo "Processing inserts from "${path_to_table}
                        table_name=${path_to_table##*/}
                        concurrent_writes=$(cat $CONF_DIR/config.json | jq '.'${KEYSPACE_NAME}'[] | select(.table_name=="'$table_name'").concurrent_writes' | tr -d '"')
                        $SPARK_HOME/spark-shell -i ${WORKING_DIR}/sbin/incremental_table_loader.scala \
                                --packages com.datastax.spark:${SPARK_CASSANDRA_CONNECTOR} \
                                --files $CONF_DIR/KeyspacesConnector.conf --conf spark.driver.memory=${DRIVER_MEMORY} \
                                --conf spark.executor.memory=${EXECUTOR_MEMORY} --conf spark.cassandra.connection.config.profile.path="KeyspacesConnector.conf" \
                                --conf spark.cassandra.output.concurrent.writes=$concurrent_writes --conf spark.cassandra.output.batch.size.rows=1 \
                                --conf spark.cassandra.output.batch.grouping.key=none --conf spark.task.maxFailures=${MAX_FAILURES} \
                                --conf spark.driver.args="${table_name} ${KEYSPACE_NAME} ${path_to_table} ${WORKING_DIR}"
                done

}

function Validate_Keyspace {
  for path_to_table in ${WORKING_DIR}/keyspaces/snapshots/$KEYSPACE_NAME/*;
                do
                        echo "Validate snapshot from the Cassandra cluster: "${path_to_table}
                        table_name=${path_to_table##*/}
                        $SPARK_HOME/spark-shell -i ${WORKING_DIR}/sbin/table_validator.scala \
                                --packages com.datastax.spark:${SPARK_CASSANDRA_CONNECTOR} \
                                --files $CONF_DIR/KeyspacesConnector.conf --conf spark.driver.memory=${DRIVER_MEMORY} \
                                --conf spark.executor.memory=${EXECUTOR_MEMORY} --conf spark.cassandra.connection.config.profile.path="KeyspacesConnector.conf" \
                                --conf spark.task.maxFailures=${MAX_FAILURES} \
                                --conf spark.driver.extraJavaOptions="-Dscala.color" \
                                --conf spark.driver.args="${table_name} ${KEYSPACE_NAME} ${path_to_table} ${WORKING_DIR}"
                done
}


(( $#<1 )) && Usage_Exit
script_name=$(basename -- "$0")

if pgrep -f spark-shell >/dev/null; then
  echo "Spark shell process is already running";
  exit 0
else
  echo "Spark process is not running. You can run $script_name";
fi

sub=$1
KEYSPACE_NAME=$2
shift

case $sub in
  --batch-keyspace|-b)
    Batch_Keyspace_Offload
    Batch_Keyspace_Load
  ;;
  --increment-keyspace|-i)
    Incremental_Keyspace_Offload
    Incremental_Keyspace_Load
  ;;
  --validate-keyspace|-v)
    Validate_Keyspace
  ;;
  *)
    Usage_Exit
  ;;
esac