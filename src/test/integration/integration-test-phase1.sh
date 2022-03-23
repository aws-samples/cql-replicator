#!/usr/bin/env bash
#
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
#
# Setup the environment
export ISENGARD_PRODUCTION_ACCOUNT=false
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN
export BUCKETNAME=cqlreplicator
export KEYSPACENAME=ks_test_cql_replicator
export TABLENAME=test_cql_replicator
export CQLREPLICATOR_CONF=$1
export PATH_TO_CST=$2
# Run one cassandra instance
docker run --name test-cql-replicator -d -e CASSANDRA_NUM_TOKENS=8 -p 9042:9042 cassandra:3.11
# Generate cassandra workload
echo "Generating C* workload"
sleep 15s
cassandra-stress user profile=$PATH_TO_CST duration=1s no-warmup ops\(insert=1\) -rate threads=2 -node 10.0.1.21
# Run CQLReplicator 2 tiles against 9 token ranges, 4 java processes
cd "$(dirname "$CQLREPLICATOR_CONF")/lib"
java -cp CQLReplicator-1.0-SNAPSHOT.jar com.amazon.aws.cqlreplicator.Init
echo "Deploying Keyspaces's tables"
sleep 60s
cd "$(dirname "$CQLREPLICATOR_CONF")/bin"
./cqlreplicator.sh --syncPartitionKeys --tile 0 --tiles 2 & ./cqlreplicator.sh --syncPartitionKeys --tile 1 --tiles 2 & \
./cqlreplicator.sh --syncClusteringColumns --tile 0 --tiles 2 & ./cqlreplicator.sh --syncClusteringColumns --tile 1 --tiles 2 &