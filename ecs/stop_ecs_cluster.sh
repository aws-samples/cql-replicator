#!/usr/bin/env bash
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
#
CLUSTER_NAME=$1
REGION_NAME=$2
ecs-cli down --cluster $CLUSTER_NAME --region $REGION_NAME

