/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class CacheStorage<K, V> {
  protected String storageName;

  public abstract void connect() throws IOException;

  public abstract void tearDown();

  public abstract V get(Object key);

  public abstract void put(K key, V value)
      throws InterruptedException, ExecutionException, TimeoutException;

  public abstract void add(int tile, K key, V value)
      throws InterruptedException, ExecutionException, TimeoutException;

  public abstract Map<K, V> getAllByTile(int tile);

  public abstract long getSize(int tile)
      throws InterruptedException, ExecutionException, TimeoutException;

  public abstract boolean containsKey(K key)
      throws InterruptedException, ExecutionException, TimeoutException;

  public abstract void remove(int tile, K key)
      throws InterruptedException, ExecutionException, TimeoutException;

  public abstract void remove(int tile, String operationType, K key)
      throws InterruptedException, ExecutionException, TimeoutException;

  public abstract Set<K> keySet();
}
