#!/usr/bin/env bash
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
#

TASK_DEF=$1
INPUT=$(($2))
TILES=$((INPUT-1))
for value in `seq 0 $TILES`
do
  while [ ! $(aws ecs list-task-definitions --family-prefix $TASK_DEF$value | grep -Eo '[0-9]+' | tail -1) = '' ];
  do
    val=$(aws ecs list-task-definitions --family-prefix $TASK_DEF$value | grep -Eo '[0-9]+' | tail -1)
    aws ecs deregister-task-definition --task-definition $TASK_DEF$value":"$val
  done
done