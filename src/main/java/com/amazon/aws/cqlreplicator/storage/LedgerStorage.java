/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import java.io.IOException;

public abstract class LedgerStorage<O, R, B> {

  public abstract void tearDown() throws IOException;

  public abstract void writePartitionMetadata(O o);

  public abstract R readPartitionMetadata(O o);

  public abstract void writeRowMetadata(O o) throws IOException;

  public abstract R readRowMetaData(O o) throws IOException;

  public abstract void deletePartitionMetadata(O o);

  public abstract void deleteRowMetadata(O o) throws IOException;

  public abstract R execute(B b);

  public abstract R readPartitionsMetadata(O o) throws IOException;
}
