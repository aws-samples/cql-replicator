// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.models;

/** Defines partition's metadata */
public class PartitionMetaData {
  private final int tile;
  private final String keyspaceName;
  private final String tableName;
  private final String pk;

  public PartitionMetaData(
      final int tile, final String keyspaceName, final String tableName, final String pk) {
    this.tile = tile;
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
    this.pk = pk;
  }

  @Override
  public String toString() {
    return "PartitionMetaData{"
        + "tile="
        + tile
        + ", keyspaceName='"
        + keyspaceName
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", pk='"
        + pk
        + '\''
        + '}';
  }

  public int getTile() {
    return tile;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPk() {
    return pk;
  }
}
