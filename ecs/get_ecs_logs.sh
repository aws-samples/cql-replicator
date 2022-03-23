#!/usr/bin/env bash#
#
# // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# // SPDX-License-Identifier: Apache-2.0
#
set -e

CLUSTER_NAME=$1
PATH_TO_PEM=$2

arr_nodes=(`aws ec2 describe-instances --filter "Name=tag:Name,Values=ECS Instance - amazon-ecs-cli-setup-${CLUSTER_NAME}" --query 'Reservations[*].Instances[*].[PublicIpAddress]' --output text`)

for item in ${arr_nodes[*]}
do
   printf "******************************************Host:   %s\n" $item
  ssh -o StrictHostKeyChecking=no -i $PATH_TO_PEM ec2-user@$item 'docker logs $(docker ps --format "{{.ID}}" --filter "name=PartitionReplicator") 2>&1 | grep -E "ERROR|WARN" | uniq'
  ssh -o StrictHostKeyChecking=no -i $PATH_TO_PEM ec2-user@$item 'docker logs $(docker ps --format "{{.ID}}" --filter "name=RowReplicator") 2>&1 | grep -E "ERROR|WARN" | uniq'
done
