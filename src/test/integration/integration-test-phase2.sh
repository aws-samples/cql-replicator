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
export TEST_CLASS=$2

# Run Unit Test
# Stopper CQLReplicator
kill $(jps -ml | grep com.amazon.aws.cqlreplicator.Starter | awk '{print $1}')
cd "$(dirname "$CQLREPLICATOR_CONF")/lib"
java -cp CQLReplicator-1.0-SNAPSHOT.jar org.junit.platform.console.ConsoleLauncher -cp $TEST_CLASS --select-class CqlReplicatorTest