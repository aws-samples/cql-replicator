/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.amazon.aws.cqlreplicator.util.Utils;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MemcachedCacheStorage extends CacheStorage<Object, Object> {

  private static final int TIMEOUT_IN_SEC = 5;
  private final String operation;
  private final String targetKeyspace;
  private final String targetTable;
  private final String externalMemcachedStorageEndpoint;
  private final String externalMemcachedStoragePort;
  private MemcachedClient memCachedClient;
  private String prefix;

  public MemcachedCacheStorage(Properties config, String operation) {
    this.operation = operation;
    this.targetKeyspace = config.getProperty("TARGET_KEYSPACE");
    this.targetTable = config.getProperty("TARGET_TABLE");
    this.externalMemcachedStorageEndpoint =
        config.getProperty("EXTERNAL_MEMCACHED_STORAGE_ENDPOINT");
    this.externalMemcachedStoragePort = config.getProperty("EXTERNAL_MEMCACHED_STORAGE_PORT");
  }

  @Override
  public void connect() throws IOException {
    var connectionConfig = new Properties();
    connectionConfig.setProperty(
        "EXTERNAL_MEMCACHED_STORAGE_ENDPOINT", externalMemcachedStorageEndpoint);
    connectionConfig.setProperty("EXTERNAL_MEMCACHED_STORAGE_PORT", externalMemcachedStoragePort);
    var connectionFactory = new ConnectionFactory(connectionConfig);
    this.memCachedClient = connectionFactory.buildMemcachedSession();
    prefix = String.format("%s|%s|%s", operation, targetKeyspace, targetTable);
  }

  public void counterIncrement(int tile)
      throws InterruptedException, ExecutionException, TimeoutException {
    var cntKey =
        new String(
            Base64.encodeBase64(
                String.format(
                        "%s|%s|%s|%s|%s", tile, operation, "counter", targetKeyspace, targetTable)
                    .getBytes()));
    var contains = memCachedClient.asyncGet(cntKey) != null;
    if (!contains) {
      memCachedClient.add(cntKey, 0, "0").get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }
    memCachedClient.asyncIncr(cntKey, 1);
  }

  private void counterDecrement(int tile, String operationType) {
    var cntKey =
        new String(
            Base64.encodeBase64(
                String.format(
                        "%s|%s|%s|%s|%s",
                        tile, operationType, "counter", targetKeyspace, targetTable)
                    .getBytes()));
    var contains = memCachedClient.get(cntKey) != null;
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
    return memCachedClient.get(
        new String(Base64.encodeBase64(String.format("%s|%s", prefix, key).getBytes())));
  }

  public int getTotalChunks(int tile) {
    var result = 0;
    var chunks =
        memCachedClient.get(
            new String(
                Base64.encodeBase64(
                    String.format(
                            "%s|%s|%s|%s|%s",
                            "pd", targetKeyspace, targetTable, tile, "totalChunks")
                        .getBytes())));

    if (chunks != null) result = Integer.parseInt((String) chunks);

    return result;
  }

  public List<Object> getListOfPartitionKeysByChunk(int chunk, int tile) throws IOException {
    var keyOfChunk =
        String.format(
            "%s|%s|%s|%s|%s|%s", "pd", targetKeyspace, targetTable, "pksChunk", tile, chunk);
    var compressedPayload =
        (byte[]) memCachedClient.get(new String(Base64.encodeBase64(keyOfChunk.getBytes())));
    var cborPayload = Utils.decompress(compressedPayload);
    return Utils.cborDecoder(cborPayload);
  }

  @Override
  public void put(Object key, Object value)
      throws InterruptedException, ExecutionException, TimeoutException {
    memCachedClient
        .set(
            new String(Base64.encodeBase64(String.format("%s|%s", prefix, key).getBytes())),
            0,
            value)
        .get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
  }

  @Override
  public void add(int tile, Object key, Object value)
      throws InterruptedException, ExecutionException, TimeoutException {
    memCachedClient
        .add(
            new String(Base64.encodeBase64(String.format("%s|%s", prefix, key).getBytes())),
            0,
            value)
        .get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    counterIncrement(tile);
  }

  @Override
  public Map getAllByTile(int tile) {
    return Collections.singletonMap("", "");
  }

  @Override
  public long getSize(int tile) throws InterruptedException, ExecutionException, TimeoutException {
    var cntKey =
        new String(
            Base64.encodeBase64(
                String.format(
                        "%s|%s|%s|%s|%s", tile, operation, "counter", targetKeyspace, targetTable)
                    .getBytes()));
    var contains = memCachedClient.asyncGet(cntKey).get(TIMEOUT_IN_SEC, TimeUnit.SECONDS) != null;
    if (!contains) {
      memCachedClient.add(cntKey, 0, "0").get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }
    return Long.parseLong(((String) memCachedClient.get(cntKey)).replace(" ", ""));
  }

  @Override
  public boolean containsKey(Object key)
      throws InterruptedException, ExecutionException, TimeoutException {
    boolean contains;
    contains =
        memCachedClient
                .asyncGet(
                    new String(Base64.encodeBase64(String.format("%s|%s", prefix, key).getBytes())))
                .get(TIMEOUT_IN_SEC, TimeUnit.SECONDS)
            != null;
    return contains;
  }

  @Override
  public void remove(int tile, Object key)
      throws InterruptedException, ExecutionException, TimeoutException {
    memCachedClient
        .delete(new String(Base64.encodeBase64(String.format("%s|%s", prefix, key).getBytes())))
        .get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    counterDecrement(tile, operation);
  }

  @Override
  public void remove(int tile, String operationType, Object key)
      throws InterruptedException, ExecutionException, TimeoutException {
    var ksAndTable = String.format("%s|%s", targetKeyspace, targetTable);
    memCachedClient
        .delete(
            new String(
                Base64.encodeBase64(
                    String.format("%s|%s|%s", operationType, ksAndTable, key).getBytes())))
        .get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    counterDecrement(tile, operationType);
  }

  @Override
  public Set keySet() {
    return null;
  }

  public void incrByOne(Object key)
      throws InterruptedException, ExecutionException, TimeoutException {
    var cntKey =
        new String(
            Base64.encodeBase64(
                String.format("%s|%s|%s|%s", "pd", targetKeyspace, targetTable, key).getBytes()));
    var contains = memCachedClient.asyncGet(cntKey).get(TIMEOUT_IN_SEC, TimeUnit.SECONDS) != null;
    if (!contains) {
      memCachedClient.add(cntKey, 0, "0").get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }
    memCachedClient.asyncIncr(cntKey, 1);
  }

  public void decrByOne(Object key) {
    var cntKey =
        new String(
            Base64.encodeBase64(
                String.format("%s|%s|%s|%s", "pd", targetKeyspace, targetTable, key).getBytes()));
    var contains = memCachedClient.get(cntKey) != null;
    if (contains) {
      memCachedClient.asyncDecr(cntKey, 1);
    }
  }
}
