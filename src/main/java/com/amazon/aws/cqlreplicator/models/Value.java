/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.models;

import java.io.Serializable;

public class Value implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long lastRun;
    private final long writeTime;
    private final String ck;

    public Value(long lastRun, long writeTime, String ck) {
        this.lastRun = lastRun;
        this.writeTime = writeTime;
        this.ck = ck;
    }

    public long getLastRun() {
        return lastRun;
    }

    public long getWriteTime() {
        return writeTime;
    }

    public String getCk() {
        return ck;
    }

}
