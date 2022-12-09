/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.models;

import java.io.Serializable;

public class PrimaryKey implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String partitionKeys;
  private final String clusteringColumns;

  public PrimaryKey(String partitionKeys, String clusteringColumns) {
    this.partitionKeys = partitionKeys;
    this.clusteringColumns = clusteringColumns;
  }

  public String getPartitionKeys() {
    return partitionKeys;
  }

  public String getClusteringColumns() {
    return clusteringColumns;
  }

  @Override
  public String toString() {
    return "PrimaryKey{" +
            "partitionKeys='" + partitionKeys + '\'' +
            ", clusteringColumns='" + clusteringColumns + '\'' +
            '}';
  }
}
