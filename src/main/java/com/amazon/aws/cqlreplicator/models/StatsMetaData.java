// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.models;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/** Defines stats metadata */
public class StatsMetaData {
  private final int tile;
  private final String keyspaceName;
  private final String tableName;
  private final String ops;
  private transient long value;

  public StatsMetaData(
      final int tile, final String keyspaceName, final String tableName, final String ops) {
    this.tile = tile;
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
    this.ops = ops;
    this.value = 0;
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

  public String getOps() {
    return ops;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long v) {
    this.value = v;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
