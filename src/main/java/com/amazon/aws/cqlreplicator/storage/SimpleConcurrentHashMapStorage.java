/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleConcurrentHashMapStorage<K, V> extends Storage<K, V> {

  private ConcurrentHashMap<K, V> storage;
  private int initialCapacity;
  private float loadFactor;
  private int concurrentThreads;

  public SimpleConcurrentHashMapStorage(Properties properties) {
    super();
    this.storageName = "SimpleConcurrentHashMapStorage";
    this.initialCapacity =
        Integer.parseInt(properties.getProperty("INTERNAL_STORAGE_INIT_CAPACITY"));
    this.loadFactor = Float.parseFloat(properties.getProperty("INTERNAL_STORAGE_LOAD_FACTOR"));
    this.concurrentThreads =
        Integer.parseInt(properties.getProperty("INTERNAL_STORAGE_CONCURRENCY_LEVEL"));
  }

  @Override
  public void connect() {
    storage = new ConcurrentHashMap<>(initialCapacity, loadFactor, concurrentThreads);
  }

  @Override
  public void tearDown() {
    storage.clear();
  }

  @Override
  public V get(Object key) {
    return storage.get(key);
  }

  @Override
  public void put(K key, V value) {
    storage.put(key, value);
  }

  @Override
  public void add(int tile, K key, V value) {
    storage.put(key, value);
  }

  @Override
  public ConcurrentHashMap<K, V> getAllByTile(int tile) {
    return this.storage;
  }

  @Override
  public long getSize(int tile) {
    return this.storage.size();
  }

  @Override
  public boolean containsKey(K key) {
    return this.storage.containsKey(key);
  }

  @Override
  public void remove(int tile, K key) {
    this.storage.remove(key);
  }

  @Override
  public void remove(int tile, String operationType, K key) {
    this.storage.remove(key);
  }

  @Override
  public Set<K> keySet() {
    return this.storage.keySet();
  }
}
