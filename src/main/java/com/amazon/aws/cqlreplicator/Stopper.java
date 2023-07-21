/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.amazon.aws.cqlreplicator.Starter.pdExecutors;
import static com.amazon.aws.cqlreplicator.Starter.rowExecutor;

/**
 * Responsible for stopping replication tasks
 */
public class Stopper implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Stopper.class);

    @Override
    public void run() {
        LOGGER.info("Stopping process is activated");
        Starter.timer.cancel();
        LOGGER.info("Replication task is stopped: {}", Starter.task.cancel());
        LOGGER.info(
                "Shutting down executors");
        rowExecutor.shutdown();
        pdExecutors.shutdown();
    }
}
