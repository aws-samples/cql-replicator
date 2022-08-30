#!/usr/bin/env bash
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
#

TILE=$1
TILES=$2
ACCOUNT=$3
EXEC_ROLE_NAME=$4
TASK_ROLE_NAME=$5
BUCKETNAME=$6
KEYSPACENAME=$7
TABLENAME=$8
IMAGE=$ACCOUNT".dkr.ecr.us-east-1.amazonaws.com/cqlreplicator:latest"
FAMILY="CQLReplicator"$TILE
ROWS_COMMAND="--syncClusteringColumns --tile $TILE --tiles $TILES"
PARTITION_COMMAND="--syncPartitionKeys --tile $TILE --tiles $TILES"
EXEC_ROLE="arn:aws:iam::"$ACCOUNT":role/"$EXEC_ROLE_NAME
TASK_ROLE="arn:aws:iam::"$ACCOUNT":role/"$TASK_ROLE_NAME

TASK_DEF=$(jq --null-input \
  --arg tile "$TILE" \
  --arg tiles "$TILES" \
  --arg bucketname "$BUCKETNAME" \
  --arg keyspacename "$KEYSPACENAME" \
  --arg tablename "$TABLENAME" \
  --arg image "$IMAGE" \
  --arg family "$FAMILY" \
  --arg rows_command "$ROWS_COMMAND" \
  --arg partition_command "$PARTITION_COMMAND" \
  --arg exec_role "$EXEC_ROLE" \
  --arg task_role "$TASK_ROLE" \
  --arg awslogname_pd "pd_tile$TILE" \
  --arg awslogname_rd "rd_tile$TILE" \
  '{
  "executionRoleArn": $exec_role,
  "requiresCompatibilities": [
    "EC2"
  ],
  "inferenceAccelerators": [],
  "containerDefinitions": [
    {
      "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "awslogs-cqlreplicator",
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix": $awslogname_rd
                }
            },
      "mountPoints": [
                {
                    "sourceVolume": "database",
                    "containerPath": "/opt",
                    "readOnly": false
                }
      ],
      "entryPoint": [
        "cqlreplicator.sh"
      ],
      "portMappings": [],
      "command": [
        $rows_command
      ],
      "cpu": 2,
      "environment": [
        {
          "name": "BUCKETNAME",
          "value": $bucketname
        },
        {
          "name": "CQLREPLICATOR_HOME",
          "value": "/root/CQLReplicator"
        },
        {
          "name": "KEYSPACENAME",
          "value": $keyspacename
        },
        {
          "name": "TABLENAME",
          "value": $tablename
        },
        {
          "name": "AWS_ACCESS_KEY_ID",
          "value": ""
        }
        ,
        {
          "name": "AWS_SESSION_TOKEN",
          "value": ""
        }
        ,
        {
          "name": "AWS_SECRET_ACCESS_KEY",
          "value": ""
        },
        {
          "name": "JAVA_OPTS",
          "value": "-Xms2048m -Xmx2048m -XX:+HeapDumpOnOutOfMemoryError"
        }
      ],
      "workingDirectory": "/root/CQLReplicator",
      "memory": 3584,
      "image": $image,
      "essential": true,
      "dockerLabels": {
        "Tile": $tile
      },
      "name": "RowReplicator"
    },
    {
    "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": "awslogs-cqlreplicator",
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix":$awslogname_pd
                }
            },
      "mountPoints": [
                {
                    "sourceVolume": "database",
                    "containerPath": "/opt",
                    "readOnly": false
                }
      ],
      "entryPoint": [
        "cqlreplicator.sh"
      ],
      "portMappings": [],
      "command": [
        $partition_command
      ],
      "cpu": 2,
      "environment": [
        {
          "name": "BUCKETNAME",
          "value": $bucketname
        },
        {
          "name": "CQLREPLICATOR_HOME",
          "value": "/root/CQLReplicator"
        },
        {
          "name": "KEYSPACENAME",
          "value": $keyspacename
        },
        {
          "name": "TABLENAME",
          "value": $tablename
        }
        ,
        {
          "name": "AWS_ACCESS_KEY_ID",
          "value": ""
        }
        ,
        {
          "name": "AWS_SESSION_TOKEN",
          "value": ""
        }
        ,
        {
          "name": "AWS_SECRET_ACCESS_KEY",
          "value": ""
        },
        {
          "name": "JAVA_OPTS",
          "value": "-Xms2048m -Xmx2048m -XX:+HeapDumpOnOutOfMemoryError"
        }
      ],
      "workingDirectory": "/root/CQLReplicator/bin",
      "memory": 3584,
      "volumesFrom": [],
      "image": $image,
      "essential": true,
      "name": "PartitionReplicator"
    }
  ],
  "volumes": [{
        "name": "database",
        "dockerVolumeConfiguration" : {
            "scope": "shared",
            "autoprovision": true,
            "driver": "local",
            "labels": {
                "database": "levelDB"
            }
        }
    }],
  "networkMode": "bridge",
  "memory": "7 gb",
  "cpu": "4 vCPU",
  "taskRoleArn": $task_role,
  "placementConstraints": [],
  "tags": [{
            "key": "name",
            "value": "CQLReplicator"
        }],
  "family": $family
}')

echo $TASK_DEF > task_def.json

aws ecs register-task-definition \
--cli-input-json file://task_def.json --output text

rm -rf task_def.json