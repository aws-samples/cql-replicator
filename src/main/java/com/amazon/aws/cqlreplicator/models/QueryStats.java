// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.models;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/** Defines metadata query in stats */
public class QueryStats {
  private final String keyspaceName;
  private final String tableName;
  private final String ops;

  public QueryStats(final String keyspaceName, final String tableName, final String ops) {
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
    this.ops = ops;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
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
}
