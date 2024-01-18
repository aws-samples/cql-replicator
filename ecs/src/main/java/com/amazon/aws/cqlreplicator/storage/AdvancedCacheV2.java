/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class AdvancedCacheV2<T> {

    private final List<T> queue = new ArrayList<>();
    private final StorageServiceImpl storage;
    private final int maxCacheSize;

    public AdvancedCacheV2(int maxCacheSize, StorageServiceImpl storage) {
        this.maxCacheSize = maxCacheSize;
        this.storage = storage;
    }

    public synchronized void put(T element)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        queue.add(element);
        if (queue.size() >= maxCacheSize) {
            doFlush();
        }
    }

    public synchronized void doFlush()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        List<T> payload = new ArrayList<>(queue);
        flush(payload, storage);
        queue.clear();
    }

    public synchronized int getSize() {
        return queue.size();
    }

    protected abstract void flush(List<T> payload, StorageServiceImpl storage)
            throws IOException, InterruptedException, ExecutionException, TimeoutException;
}
