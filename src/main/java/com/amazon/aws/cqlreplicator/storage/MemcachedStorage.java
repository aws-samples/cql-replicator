/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MemcachedStorage extends Storage {

    private MemcachedClient memCachedClient;
    private String prefix;
    private String operation;
    private final static int TIMEOUT_IN_SEC = 5;
    private String targetKeyspace;
    private String targetTable;
    private String externalMemcachedStorageEndpoint;
    private String externalMemcachedStoragePort;

    public MemcachedStorage(Properties config, String operation) {
        this.operation = operation;
        this.targetKeyspace = config.getProperty("TARGET_KEYSPACE");
        this.targetTable = config.getProperty("TARGET_TABLE");
        this.externalMemcachedStorageEndpoint = config.getProperty("EXTERNAL_MEMCACHED_STORAGE_ENDPOINT");
        this.externalMemcachedStoragePort = config.getProperty("EXTERNAL_MEMCACHED_STORAGE_PORT")  ;
    }

    @Override
    public void connect() throws IOException {
        Properties connectionConfig = new Properties();
        connectionConfig.setProperty("EXTERNAL_MEMCACHED_STORAGE_ENDPOINT", externalMemcachedStorageEndpoint);
        connectionConfig.setProperty("EXTERNAL_MEMCACHED_STORAGE_PORT", externalMemcachedStoragePort);
        ConnectionFactory connectionFactory = new ConnectionFactory(connectionConfig);
            this.memCachedClient = connectionFactory.getMemcachedConnection();
        prefix = String.format("%s|%s|%s", operation, targetKeyspace, targetTable);
    }

    public void counterIncrement(int tile) throws InterruptedException, ExecutionException, TimeoutException {
        String cntKey = String.format("%s|%s|%s|%s|%s",tile, operation, "counter", targetKeyspace, targetTable);
        boolean contains = memCachedClient.asyncGet(cntKey) != null;
        if (contains) {
            memCachedClient.asyncIncr(cntKey, 1);
        } else {
                memCachedClient.add(cntKey, 0, "0").get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
            memCachedClient.asyncIncr(cntKey, 1);
        }
    }

    private void counterDecrement(int tile, String operationType) {
        String cntKey = String.format("%s|%s|%s|%s|%s",tile, operationType, "counter", targetKeyspace, targetTable);
        boolean contains = memCachedClient.get(cntKey) != null;
        if (contains) {
            memCachedClient.asyncDecr(cntKey, 1);

        }
    }

    @Override
    public void tearDown() {
        memCachedClient.shutdown();
    }

    @Override
    public Object get(Object key) {
        return memCachedClient.get(String.format("%s|%s", prefix, key));
    }

    @Override
    public void put(Object key, Object value) throws InterruptedException, ExecutionException, TimeoutException {
            memCachedClient.set(String.format("%s|%s", prefix, key), 0, value).get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }

    @Override
    public void add(int tile, Object key, Object value) throws InterruptedException, ExecutionException, TimeoutException {
            memCachedClient.add(String.format("%s|%s", prefix, key), 0, value).get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
        counterIncrement(tile);

    }

    @Override
    public Map getAllByTile(int tile) {
        return Collections.singletonMap("","");
    }

    @Override
    public long getSize(int tile) throws InterruptedException, ExecutionException, TimeoutException {
        String cntKey = String.format("%s|%s|%s|%s|%s",tile, operation, "counter", targetKeyspace, targetTable);
        boolean contains;
            contains = memCachedClient.asyncGet(cntKey).get(TIMEOUT_IN_SEC, TimeUnit.SECONDS) != null;
        if (!contains) {
                memCachedClient.add(cntKey, 0, "0").get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
        }
        return Long.parseLong( ((String)memCachedClient.get(cntKey)).replace(" ", ""));
    }

    @Override
    public boolean containsKey(Object key) throws InterruptedException, ExecutionException, TimeoutException {
        boolean contains = false;
      contains =
          memCachedClient.asyncGet(String.format("%s|%s", prefix, key)).get(TIMEOUT_IN_SEC, TimeUnit.SECONDS)
              != null;
        return contains;
    }

    @Override
    public void remove(int tile, Object key) throws InterruptedException, ExecutionException, TimeoutException {
            memCachedClient.delete(String.format("%s|%s", prefix, key)).get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
        counterDecrement(tile, operation);
    }

    @Override
    public void remove(int tile, String operationType, Object key) throws InterruptedException, ExecutionException, TimeoutException {
        String ksAndTable = String.format("%s|%s", targetKeyspace, targetTable);
            memCachedClient.delete(String.format("%s|%s|%s",operationType, ksAndTable, key)).get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
        counterDecrement(tile, operationType);
    }


    @Override
    public Set keySet() {
        return null;
    }

    public void incrByOne(Object key) throws InterruptedException, ExecutionException, TimeoutException {
        String cntKey = String.format("%s|%s|%s|%s","pd", targetKeyspace, targetTable,key);
        boolean contains = memCachedClient.asyncGet(cntKey).get(TIMEOUT_IN_SEC, TimeUnit.SECONDS) != null;
        if (contains) {
            memCachedClient.asyncIncr(cntKey, 1);
        } else {
                memCachedClient.add(cntKey, 0, "0").get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
            memCachedClient.asyncIncr(cntKey, 1);
        }
    }

    public void decrByOne(Object key) {
        String cntKey = String.format("%s|%s|%s|%s","pd", targetKeyspace, targetTable,key);
        boolean contains = memCachedClient.get(cntKey) != null;
        if (contains) {
            memCachedClient.asyncDecr(cntKey, 1);

        }

    }

}
