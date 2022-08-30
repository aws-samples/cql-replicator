#!/usr/bin/env bash
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

# Pre-flight check
if ! command -v jq &> /dev/null
then
    echo "jq could not be found"
    exit
fi

if ! command -v aws &> /dev/null
then
    echo "aws could not be found"
    exit
fi

if ! command -v ecs-cli &> /dev/null
then
    echo "ecs-cli could not be found"
    exit
fi

TILES=$(($1-1))
# Register ECS Tasks
for value in `seq 0 $TILES`
do
  echo "Registering the task"
  ./register_ecs_task_def.sh $value $@
done

# Create a CloudWatch log-group
aws logs create-log-group --log-group-name awslogs-cqlreplicator

SUBNETS=$8
VPC=$9
SG=${10}
KEYPAIR_NAME=${11}
CLUSTER_NAME=$(echo ${7} | tr _ -)

# Start the ECS cluster
./start_ecs_cluster.sh $1 $SUBNETS $VPC $SG $KEYPAIR_NAME $CLUSTER_NAME



