#!/usr/bin/env bash
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

# If using AWS SMPS to store credentials, uncomment the following line:
#export AWS_SMPS_MODE=true

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
#Copy config.yaml file
aws s3 cp s3://"$BUCKETNAME"/"$KEYSPACENAME"/"$TABLENAME"/config.properties "$CQLREPLICATOR_HOME"/conf/
#Copy CassandraConnector.conf
aws s3 cp s3://"$BUCKETNAME"/"$KEYSPACENAME"/"$TABLENAME"/CassandraConnector.conf "$CQLREPLICATOR_HOME"/conf/
#Copy KeyspacesConnector.conf
aws s3 cp s3://"$BUCKETNAME"/"$KEYSPACENAME"/"$TABLENAME"/KeyspacesConnector.conf "$CQLREPLICATOR_HOME"/conf/

# Updating credentials for Cassandra and Keyspaces from Parameter Store (If using other credential manager, update accordingly)

if [ -n "${AWS_SMPS_MODE}" ]; then

USERNAME1=$(grep 'username' $CQLREPLICATOR_HOME/conf/KeyspacesConnector.conf | cut -d '=' -f2)
PASSWORD1=$(aws ssm get-parameter --name $USERNAME1 --with-decryption | jq '.Parameter.Value')
sed -i '' -e "s#password = .*#password = $PASSWORD1#" $CQLREPLICATOR_HOME/conf/KeyspacesConnector.conf

USERNAME2=$(grep 'username' $CQLREPLICATOR_HOME/conf/CassandraConnector.conf | cut -d '=' -f2)
PASSWORD2=$(aws ssm get-parameter --name $USERNAME2 --with-decryption | jq '.Parameter.Value')
sed -i '' -e "s#password = .*#password = $PASSWORD2#" $CQLREPLICATOR_HOME/conf/CassandraConnector.conf

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
