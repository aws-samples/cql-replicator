/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

public abstract class LedgerStorage<O, R, B> {

  public abstract void tearDown();

  public abstract void writePartitionMetadata(O o);

  public abstract R readPartitionMetadata(O o);

  public abstract void writeRowMetadata(O o);

  public abstract R readRowMetaData(O o);

  public abstract void deletePartitionMetadata(O o);

  public abstract void deleteRowMetadata(O o);

  public abstract R execute(B b);

  public abstract R readPartitionsMetadata(O o);
}
