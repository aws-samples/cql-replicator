#!/usr/bin/env bash
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

# Use below steps only if CQLReplicator runs on EC2 instances
#basefolder=$(pwd -L)
#export CQLREPLICATOR_HOME=${basefolder:0:${#basefolder}-4}

export CQLREPLICATOR_CONF=$CQLREPLICATOR_HOME"/conf"
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export JARS_PATH=$CQLREPLICATOR_HOME/lib/*

echo "CQLREPLICATOR_HOME:"$CQLREPLICATOR_HOME
echo "CQLREPLICATOR_CONF:"$CQLREPLICATOR_CONF

for file in $JARS_PATH; do
    classpath+=$file":"
done
#Copy the config.properties file
aws s3 cp s3://"$BUCKETNAME"/"$KEYSPACENAME"/"$TABLENAME"/config.properties "$CQLREPLICATOR_HOME"/conf/
#Copy the CassandraConnector.conf
aws s3 cp s3://"$BUCKETNAME"/"$KEYSPACENAME"/"$TABLENAME"/CassandraConnector.conf "$CQLREPLICATOR_HOME"/conf/
#Copy the KeyspacesConnector.conf
aws s3 cp s3://"$BUCKETNAME"/"$KEYSPACENAME"/"$TABLENAME"/KeyspacesConnector.conf "$CQLREPLICATOR_HOME"/conf/

# Updating credentials for Cassandra and Amazon Keyspaces from AWS System Manager Property Store

if [ -n "${AWS_SMPS_MODE}" ]; then
  for filename in "KeyspacesConnector.conf" "CassandraConnector.conf"; do
    DB_USERNAME=$(grep 'username' $CQLREPLICATOR_HOME/conf/$filename | cut -d '=' -f2)
    DB_PASSWORD=$(aws ssm get-parameter --name $DB_USERNAME --with-decryption | jq '.Parameter.Value' | sed 's/\//\\\//g')
    sed -i -e 's/password.*/password = '$DB_PASSWORD'/g' $CQLREPLICATOR_HOME/conf/$filename
  done
fi 

if [ -n "${BUCKETNAME}" ] && [ -n "${KEYSPACENAME}" ] && [ -n "${TABLENAME}" ]; then
  if [ -z "${DEV_MODE}" ]; then
    # shellcheck disable=SC2068
    java $JAVA_OPTS -Dlogback.configurationFile="$CQLREPLICATOR_CONF"/logback.xml -cp "$classpath" -Dcom.datastax.driver.USE_NATIVE_CLOCK=false com.amazon.aws.cqlreplicator.Starter $@
  fi

  if [ -n "${DEV_MODE}" ]; then
    java -Xmx2g -Xms2g -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4001 -Dlogback.configurationFile="$CQLREPLICATOR_CONF"/logback.xml -cp "$classpath" -Dcom.datastax.driver.USE_NATIVE_CLOCK=false com.amazon.aws.cqlreplicator.Starter $@
  fi
fi
