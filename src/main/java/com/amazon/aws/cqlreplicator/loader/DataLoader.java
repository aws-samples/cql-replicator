// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.loader;

/** Load data to a target table */
public interface DataLoader {
  void load(Object... object);
}
