#!/usr/bin/env bash
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

# Use below steps only if CQLReplicator runs on EC2 instances
#basefolder=$(pwd -L)
#export CQLREPLICATOR_HOME=${basefolder:0:${#basefolder}-4}

export CQLREPLICATOR_CONF=$CQLREPLICATOR_HOME"/conf"
export JARS_PATH=$CQLREPLICATOR_HOME/lib/*

echo "CQLREPLICATOR_HOME:"$CQLREPLICATOR_HOME
echo "CQLREPLICATOR_CONF:"$CQLREPLICATOR_CONF

LOCKED_TILE=""
EPOCH=$(date +%s%3N)

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

# Distributed lock based on S3
# Create a lock
aws s3api put-object --bucket $BUCKETNAME --key "$KEYSPACENAME"/"$TABLENAME"/tiles/"$EPOCH".lock
sleep 1
echo "Lock file starts with "$EPOCH
# Initial value
COUNTER=$(aws s3api list-objects --bucket $BUCKETNAME --prefix "$KEYSPACENAME"/"$TABLENAME"/tiles --query "length(Contents)")
# Wait until all ECS tasks put lock objects into /tiles
while [ $COUNTER -lt $TILES ]
do
  # Update the counter
  echo "Distributed counter:" $COUNTER
  COUNTER=$(aws s3api list-objects --bucket $BUCKETNAME --prefix "$KEYSPACENAME"/"$TABLENAME"/tiles --query "length(Contents)")
  sleep 1
done

echo "The lock is removed"

# Get the list of locks
str=$(aws s3api list-objects --bucket cqlreplicator --prefix "$KEYSPACENAME"/"$TABLENAME"/tiles --query 'Contents[].Key'| tr -d '"[]')
new_str=$(echo $str | tr -d ' ')
IFS=',' read -ra tmp_locks <<< "$new_str"
locks=("${tmp_locks[@]:1}")
# Start with the tile 0
pos=0
# Acquire the tile
for lock in "${locks[@]}"
do
   lock_file=${lock##*/}
   echo $lock_file
   # Take the position as TILE
   if [[ $lock_file == $EPOCH* ]]; then
     echo "Found the locked tile:" $lock_file
     LOCKED_TILE=$pos
     break
   fi
   pos=$(( $pos + 1 ))
done

echo "Current tile is "$LOCKED_TILE

if [ -n "${BUCKETNAME}" ] && [ -n "${KEYSPACENAME}" ] && [ -n "${TABLENAME}" ] && [ -n "${LOCKED_TILE}" ]; then
  echo Starting the task for tile $LOCKED_TILE
  java $JAVA_OPTS -Dlogback.configurationFile="$CQLREPLICATOR_CONF"/logback.xml -cp "$classpath" -Dcom.datastax.driver.USE_NATIVE_CLOCK=false com.amazon.aws.cqlreplicator.Starter --tile "$LOCKED_TILE" --tiles "$TILES"
else
  echo "Releasing the lock file due an issue"
  aws s3api delete-object --bucket $BUCKETNAME --key "$KEYSPACENAME"/"$TABLENAME"/tiles/"$EPOCH".lock
fi

