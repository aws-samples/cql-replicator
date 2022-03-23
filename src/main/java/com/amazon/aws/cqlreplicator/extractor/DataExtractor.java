// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.extractor;

import java.util.List;

/**
 * Provides interface for data extractor. The user of this interface can access multiple data
 * sources and extract data as List of generic items, for example, DynamoDB items or Cassandra rows.
 */
public interface DataExtractor {
  List<?> extract(Object object);
}
