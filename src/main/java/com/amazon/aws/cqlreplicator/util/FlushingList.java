/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.util;

import java.util.ArrayList;
import java.util.List;

public abstract class FlushingList<T> {

    private final List<T> queue = new ArrayList<>();
    private final int maxCacheSize;

    public FlushingList(final int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    public synchronized void put(T element) {
        queue.add(element);
        if (queue.size() >= maxCacheSize) {
            doFlush();
        }
    }

    public synchronized void doFlush() {
        List<T> payload = new ArrayList<>(queue);
        flush(payload);
        queue.clear();
    }

    public synchronized int getSize() {
        return queue.size();
    }

    protected abstract void flush(List<T> payload);
}
