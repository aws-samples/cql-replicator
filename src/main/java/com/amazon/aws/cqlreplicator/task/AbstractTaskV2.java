// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task;

import com.amazon.aws.cqlreplicator.storage.StorageServiceImpl;
import com.amazon.aws.cqlreplicator.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Responsible for abstracting replication logic between source table and target table
 */
public abstract class AbstractTaskV2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTaskV2.class);

    public final void performTask(StorageServiceImpl storageService, Utils.CassandraTaskTypes taskName)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        var startTime = System.nanoTime();
        doPerformTask(storageService, taskName);
        var elapsedTime = System.nanoTime() - startTime;
        LOGGER.debug(
                "Elapsed time is {} ms for task {}", Duration.ofNanos(elapsedTime).toMillis(), taskName);
    }


    protected abstract void doPerformTask(StorageServiceImpl storageService, Utils.CassandraTaskTypes taskName)
            throws IOException, InterruptedException, ExecutionException, TimeoutException;
}
