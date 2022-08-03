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

public abstract class AdvancedCache<T> {

  private final List<T> queue = new ArrayList<>();
  private CacheStorage cacheStorage;
  private int maxCacheSize;

  public AdvancedCache(int maxCacheSize, CacheStorage cacheStorage) {
    this.maxCacheSize = maxCacheSize;
    this.cacheStorage = cacheStorage;
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
    flush(payload, cacheStorage);
    queue.clear();
  }

  public synchronized int getSize() {
    return queue.size();
  }

  protected abstract void flush(List<T> payload, CacheStorage cacheStorage)
      throws IOException, InterruptedException, ExecutionException, TimeoutException;
}
