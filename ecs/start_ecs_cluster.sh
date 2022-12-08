#!/usr/bin/env bash
# /#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# /#SPDX-License-Identifier: Apache-2.0
#
set -e

INPUT=$(($1))
TILES=$((INPUT-1))
SUBNETS=$2
VPC=$3
SG=$4
KEYPAIR_NAME=$5
CLUSTER_NAME=$6

ecs-cli configure --cluster $CLUSTER_NAME --region us-east-1

ecs-cli up --force --keypair $KEYPAIR_NAME --capability-iam \
           --size $INPUT --instance-type a1.xlarge --subnets $SUBNETS \
           --vpc $VPC --security-group $SG --region us-east-1 --launch-type EC2

# Wait 60 seconds until the cluster is ready
sleep 60

# Provision tasks
for value in `seq 0 $TILES`
do
  echo "Starting CQLReplicator"$value
  echo $CLUSTER_NAME$value
  aws ecs run-task --cluster $CLUSTER_NAME --task-definition CQLReplicator$value --output text
done
