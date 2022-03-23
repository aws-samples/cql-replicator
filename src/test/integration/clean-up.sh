#!/usr/bin/env bash
#
# // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# // SPDX-License-Identifier: Apache-2.0
#
# Cleanup
kill $(jps -ml | grep com.amazon.aws.cqlreplicator.Starter | awk '{print $1}')
docker container stop test-cql-replicator
docker container rm test-cql-replicator