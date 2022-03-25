// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.models;

/** Defines retry metadata */
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.time.LocalDate;

public class RetryEntry {

  private final String partitionKey;
  private final String clusteringColumns;
  private final String keyspaceName;
  private final String tableName;
  private final String ops;
  private final LocalDate dt_retries;

  public RetryEntry(
      final String partitionKey,
      final String clusteringColumns,
      final String keyspaceName,
      final String tableName,
      final String ops,
      final LocalDate dt_retries) {
    this.partitionKey = partitionKey;
    this.clusteringColumns = clusteringColumns;
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
    this.ops = ops;
    this.dt_retries = dt_retries;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public String getClusteringColumns() {
    return clusteringColumns;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getOps() {
    return ops;
  }

  public LocalDate getDt_retries() {
    return dt_retries;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
