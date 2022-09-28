/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.models;

import java.io.Serializable;

public class TimestampMetrics implements Serializable {
  private static final long serialVersionUID = 1L;
  private final long lastRun;
  private final long writeTime;

  public TimestampMetrics(long lastRun, long writeTime) {
    this.lastRun = lastRun;
    this.writeTime = writeTime;
  }

  public long getLastRun() {
    return lastRun;
  }

  public long getWriteTime() {
    return writeTime;
  }
}
