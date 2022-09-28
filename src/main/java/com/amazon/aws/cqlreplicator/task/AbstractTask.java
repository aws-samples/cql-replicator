// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task;

import com.amazon.aws.cqlreplicator.storage.CacheStorage;
import com.amazon.aws.cqlreplicator.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** Responsible for abstracting replication logic between source table and target table */
public abstract class AbstractTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTask.class);

  public final void performTask(CacheStorage pkCache, Utils.CassandraTaskTypes taskName)
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    var startTime = System.nanoTime();
    doPerformTask(pkCache, taskName);
    var elapsedTime = System.nanoTime() - startTime;
    LOGGER.info(
        "Elapsed time is {} ms for task {}", Duration.ofNanos(elapsedTime).toMillis(), taskName);
  }


  protected abstract void doPerformTask(CacheStorage pkCache, Utils.CassandraTaskTypes taskName)
      throws IOException, InterruptedException, ExecutionException, TimeoutException;
}
