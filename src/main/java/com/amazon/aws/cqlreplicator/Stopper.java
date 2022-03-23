/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Responsible for stopping replication tasks */
public class Stopper extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(Stopper.class);

  @Override
  public void run() {
    LOGGER.info("Stopping process is activated");
    Starter.timer.cancel();
    LOGGER.info("Replication task is stopped: {}", Starter.task.cancel());
  }
}
