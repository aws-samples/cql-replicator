// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.models;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/** Defines multiple partitions' metadata */
public class PartitionsMetaData {

  private final int tile;
  private final String keyspaceName;
  private final String tableName;

  public PartitionsMetaData(final int tile, final String keyspaceName, final String tableName) {
    this.tile = tile;
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
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
