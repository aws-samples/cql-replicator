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

abstract public class Storage<K,V> {
    protected String storageName;

    abstract public void connect() throws IOException;
    abstract public void tearDown();
    abstract public V get(Object key);
    abstract public void put(K key, V value) throws InterruptedException, ExecutionException, TimeoutException;
    abstract public void add(int tile, K key, V value) throws InterruptedException, ExecutionException, TimeoutException;
    abstract public Map<K, V> getAllByTile(int tile);
    abstract public long getSize(int tile) throws InterruptedException, ExecutionException, TimeoutException;
    abstract public boolean containsKey(K key) throws InterruptedException, ExecutionException, TimeoutException;
    abstract public void remove(int tile, K key) throws InterruptedException, ExecutionException, TimeoutException;
    abstract public void remove(int tile, String operationType, K key) throws InterruptedException, ExecutionException, TimeoutException;
    abstract public Set<K> keySet();

}
