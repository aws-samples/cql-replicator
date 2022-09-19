/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.models;

import java.io.Serializable;

public class PartitionKey implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String partitionKey;

  public PartitionKey(String partitionKeys) {
    this.partitionKey = partitionKeys;
  }

  public String getPartitionKey() {

    return partitionKey;
  }
}
