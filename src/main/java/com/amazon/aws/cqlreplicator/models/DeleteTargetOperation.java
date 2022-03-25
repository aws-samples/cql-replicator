// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.models;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.util.Arrays;
import java.util.Map;

/** Defines deletion in target and ledger */
public class DeleteTargetOperation {
  private final String keyspaceName;
  private final String tableName;
  private final String[] values;
  private final String[] names;
  private final Map<String, String> types;

  public DeleteTargetOperation(
      final String keyspaceName,
      final String tableName,
      final String[] values,
      final String[] names,
      final Map<String, String> types) {
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
    this.values = values;
    this.names = names;
    this.types = types;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String[] getValues() {
    return values;
  }

  public String[] getNames() {
    return names;
  }

  public Map<String, String> getTypes() {
    return types;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
