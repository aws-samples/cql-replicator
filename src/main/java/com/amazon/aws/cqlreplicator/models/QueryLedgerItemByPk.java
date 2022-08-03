// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.models;

/** Defines metadata query in ledger */
public class QueryLedgerItemByPk {
  private final String partitionKey;
  private final int tile;
  private final String keyspaceName;
  private final String tableName;

  public QueryLedgerItemByPk(
      final String partitionKey,
      final int tile,
      final String keyspaceName,
      final String tableName) {

    this.partitionKey = partitionKey;
    this.tile = tile;
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
  }

  public String getPartitionKey() {
    return partitionKey;
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
}
