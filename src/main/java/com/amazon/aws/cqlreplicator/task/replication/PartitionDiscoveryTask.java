// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.models.*;
import com.amazon.aws.cqlreplicator.storage.*;
import com.amazon.aws.cqlreplicator.storage.SimpleConcurrentHashMapCacheStorage;
import com.amazon.aws.cqlreplicator.storage.CacheStorage;
import com.amazon.aws.cqlreplicator.task.AbstractTask;
import com.amazon.aws.cqlreplicator.util.StatsCounter;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

//import static com.amazon.aws.cqlreplicator.util.Utils.alignRangesAndTiles;
import static com.amazon.aws.cqlreplicator.util.Utils.getDistributedRangesByTiles;

/**
 * The {@code PartitionDiscoveryTask} class provides partition key synchronization between Cassandra
 * cluster and Amazon Keyspaces by using <em>token range split</em>. This implementation makes ~
 * &scanAndCompare; scan Cassandra cluster and compare any token range of <em>n</em>.
 *
 * <p>The {@code PartitionDiscoveryTask} splits Cassandra token range in <em>m</em> tiles and each
 * instance of PartitionDiscoveryTask handles only one tile
 *
 * <p>
 */
public class PartitionDiscoveryTask extends AbstractTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionDiscoveryTask.class);
  private static final Pattern REGEX_PIPE = Pattern.compile("\\|");
  private static final int REQUEST_PER_PARTITION = 1000;
  private static final int RATE_LIMITER_TIMEOUT_MS = 1000;
  private static final int ADVANCED_CACHE_SIZE = 1000;
  private static SourceStorageOnCassandra sourceStorageOnCassandra;
  private static Map<String, LinkedHashMap<String, String>> metaData;
  private final Properties config;
  private static StatsCounter statsCounter;
  private static LedgerStorageOnKeyspaces ledgerStorageOnKeyspaces;
  private static TargetStorageOnKeyspaces targetStorageOnKeyspaces;

  /**
   * Constructor for PartitionDiscoveryTask.
   *
   * @param config the array to be sorted
   */
  public PartitionDiscoveryTask(Properties config) {
    this.config = config;
    sourceStorageOnCassandra = new SourceStorageOnCassandra(config);
    metaData = sourceStorageOnCassandra.getMetaData();
    statsCounter = new StatsCounter();
    ledgerStorageOnKeyspaces = new LedgerStorageOnKeyspaces(config);
    targetStorageOnKeyspaces = new TargetStorageOnKeyspaces(config);
  }

  /** Scan and compare partition keys. */
  private void scanAndCompare(
          List<ImmutablePair<String, String>> rangeList, CacheStorage pkCache, String[] pks)
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    AdvancedCache<String> advancedCache = null;

    if (pkCache instanceof MemcachedCacheStorage) {
      advancedCache =
          new AdvancedCache<>(ADVANCED_CACHE_SIZE, pkCache) {
            @Override
            protected void flush(List<String> payload, CacheStorage cacheStorage)
                throws IOException, InterruptedException, ExecutionException, TimeoutException {
              String totalChunks =
                  String.format("%s|%s", config.getProperty("TILE"), "totalChunks");
              int currentChunk = Integer.parseInt((String) cacheStorage.get(totalChunks));
              LOGGER.debug("{}:{}", totalChunks, currentChunk);
              byte[] cborPayload = Utils.cborEncoder(payload);
              byte[] compressedCborPayload = Utils.compress(cborPayload);
              String keyOfChunk =
                  String.format("%s|%s|%s", "pksChunk", config.getProperty("TILE"), currentChunk);
              pkCache.put(keyOfChunk, compressedCborPayload);
              ((MemcachedCacheStorage) pkCache).incrByOne(totalChunks);
            }
          };
    }

    boolean totalChunksExist =
        pkCache.containsKey(String.format("%s|%s", config.getProperty("TILE"), "totalChunks"));
    if (!totalChunksExist)
      pkCache.put(String.format("%s|%s", config.getProperty("TILE"), "totalChunks"), "0");

    RateLimiter rateLimiter = RateLimiter.create(REQUEST_PER_PARTITION);
    String pksStr = String.join(",", pks);

    for (ImmutablePair<String, String> range : rangeList) {
      long rangeStart = Long.parseLong(range.left);
      long rangeEnd = Long.parseLong(range.right);

      List<Row> resultSetRange =
              sourceStorageOnCassandra.findPartitionsByTokenRange(pksStr, rangeStart, rangeEnd);

      LOGGER.trace("Processing a range: {} - {}", rangeStart, rangeEnd);
      for (Row eachResult : resultSetRange) {
        int i = 0;
        List<String> tmp = new ArrayList<>();

        for (String cl : pks) {
          String type = metaData.get("partition_key").get(cl);
          tmp.add(String.valueOf(eachResult.get(pks[i], Utils.getClassType(type.toUpperCase()))));
          i++;
        }

        String res = String.join("|", tmp);
        boolean flag = pkCache.containsKey(res);

        if (!flag) {
          pkCache.add(
              Integer.parseInt(config.getProperty("TILE")), res, Instant.now().toEpochMilli());

          PartitionMetaData partitionMetaData =
              new PartitionMetaData(
                  Integer.parseInt(config.getProperty("TILE")),
                  config.getProperty("TARGET_KEYSPACE"),
                  config.getProperty("TARGET_TABLE"),
                  res);

          syncPartitionKeys(partitionMetaData, rateLimiter);

          if (advancedCache != null) advancedCache.put(partitionMetaData.getPk());

          LOGGER.debug("Syncing a new partition key: {}", res);
        }
      }
    }

    if (advancedCache != null && advancedCache.getSize() > 0) {
      LOGGER.info("Flushing remainders: {}", advancedCache.getSize());
      advancedCache.doFlush();
    }

    LOGGER.info("Comparing stage is running");
  }

  private void syncPartitionKeys(
      PartitionMetaData partitionMetaData, RateLimiter rateLimiter) {
    rateLimiter.tryAcquire(1, RATE_LIMITER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    ledgerStorageOnKeyspaces.writePartitionMetadata(partitionMetaData);

  }

  private void removePartitions(String[] pks, CacheStorage pkCache, int chunk)
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    Collection<?> collection = null;

    if (pkCache instanceof MemcachedCacheStorage) {

      String keyOfChunk = String.format("%s|%s|%s", "pksChunk", config.getProperty("TILE"), chunk);
      byte[] compressedPayload = (byte[]) pkCache.get(keyOfChunk);
      byte[] cborPayload;
      cborPayload = Utils.decompress(compressedPayload);
      collection = Utils.cborDecoder(cborPayload);
    }

    if (pkCache instanceof SimpleConcurrentHashMapCacheStorage) {
      collection = pkCache.keySet();
    }

    Collection<?> finalClonedCollection = new ArrayList<>(collection);
    collection.stream()
        .forEach(
            key -> {
              String[] pk;
              BoundStatementBuilder boundStatementCassandraBuilder =
                      sourceStorageOnCassandra.getCassandraPreparedStatement().boundStatementBuilder();
              if (pkCache instanceof MemcachedCacheStorage)
                LOGGER.debug("Processing partition key: {}", key);
              pk = REGEX_PIPE.split((String) key);

              if (pkCache instanceof SimpleConcurrentHashMapCacheStorage)
                pk = REGEX_PIPE.split((String) key);

              int i = 0;

              for (String cl : pks) {
                String type = metaData.get("partition_key").get(cl);
                try {
                  boundStatementCassandraBuilder =
                      Utils.aggregateBuilder(type, cl, pk[i], boundStatementCassandraBuilder);
                } catch (Exception e) {
                  LOGGER.error(e.getMessage());
                }
                i++;
              }
              List<Row> cassandraResult =
                      sourceStorageOnCassandra.extract(boundStatementCassandraBuilder);
              // Found deleted key
              if (cassandraResult.size() == 0) {
                LOGGER.debug("Found deleted partition key {}", key);
                // Delete from Ledger

                QueryLedgerItemByPk queryLedgerItemByPk = null;
                PartitionMetaData partitionMetaData = null;

                if (pkCache instanceof MemcachedCacheStorage) {
                  queryLedgerItemByPk =
                      new QueryLedgerItemByPk(
                          (String) key,
                          Integer.parseInt(config.getProperty("TILE")),
                          config.getProperty("TARGET_KEYSPACE"),
                          config.getProperty("TARGET_TABLE"));
                }

                if (pkCache instanceof SimpleConcurrentHashMapCacheStorage) {
                  queryLedgerItemByPk =
                      new QueryLedgerItemByPk(
                          (String) key,
                          Integer.parseInt(config.getProperty("TILE")),
                          config.getProperty("TARGET_KEYSPACE"),
                          config.getProperty("TARGET_TABLE"));

                  partitionMetaData =
                      new PartitionMetaData(
                          Integer.parseInt(config.getProperty("TILE")),
                          config.getProperty("TARGET_KEYSPACE"),
                          config.getProperty("TARGET_TABLE"),
                          (String) key);
                }
                // Remove clustering columns associated with the partition key from the cache
                if (pkCache instanceof MemcachedCacheStorage) {
                  List<Row> ledgerResultSet = ledgerStorageOnKeyspaces.readRowMetaData(queryLedgerItemByPk);
                  ledgerResultSet.stream()
                      .forEach(
                          deleteRow -> {
                            try {
                              pkCache.remove(
                                  Integer.parseInt(config.getProperty("TILE")),
                                  "rd",
                                  String.format(
                                      "%s|%s", ((String) key), deleteRow.getString("cc")));
                            } catch (InterruptedException
                                | ExecutionException
                                | TimeoutException e) {
                              throw new RuntimeException(e);
                            }
                          });

                  partitionMetaData =
                      new PartitionMetaData(
                          Integer.parseInt(config.getProperty("TILE")),
                          config.getProperty("TARGET_KEYSPACE"),
                          config.getProperty("TARGET_TABLE"),
                          ((String) key));
                }

                // Delete from Target table
                DeleteTargetOperation deleteTargetOperation =
                    new DeleteTargetOperation(
                        config.getProperty("TARGET_KEYSPACE"),
                        config.getProperty("TARGET_TABLE"),
                        pk,
                        pks,
                        metaData.get("partition_key"));
                ledgerStorageOnKeyspaces.deletePartitionMetadata(partitionMetaData);
                ledgerStorageOnKeyspaces.deleteRowMetadata(queryLedgerItemByPk);
                targetStorageOnKeyspaces.delete(deleteTargetOperation);

                // Remove partition key from the cache
                if (pkCache instanceof MemcachedCacheStorage) {
                  try {
                    pkCache.remove(Integer.parseInt(config.getProperty("TILE")), ((String) key));
                  } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                  }
                  finalClonedCollection.remove(key);
                }

                if (pkCache instanceof SimpleConcurrentHashMapCacheStorage) {
                  try {
                    pkCache.remove(Integer.parseInt(config.getProperty("TILE")), ((String) key));
                  } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                  }
                }

                statsCounter.incrementStat("DELETE");
              }
            });
    if (pkCache instanceof MemcachedCacheStorage && finalClonedCollection.size() < collection.size()) {
      byte[] cborPayload = Utils.cborEncoder((List<String>) finalClonedCollection);
      byte[] compressedPayload = Utils.compress(cborPayload);
      String keyOfChunk = String.format("%s|%s|%s", "pksChunk", config.getProperty("TILE"), chunk);
      pkCache.put(keyOfChunk, compressedPayload);
    }

    if (pkCache instanceof MemcachedCacheStorage && finalClonedCollection.size() == 0) {
      String keyOfChunk = String.format("%s|%s", config.getProperty("TILE"), "totalChunks");
      ((MemcachedCacheStorage) pkCache).decrByOne(keyOfChunk);
    }
  }

  /**
   * Scan and remove deleted partition keys.
   *
   * @params rangeList, pkCache, pks the array to be sorted
   */
  private void scanAndRemove(CacheStorage pkCache, String[] pks, Utils.CassandraTaskTypes taskName)
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    if (taskName.equals(Utils.CassandraTaskTypes.SYNC_DELETED_PARTITION_KEYS)) {
      LOGGER.info("Syncing deleted partition keys between C* and Amazon Keyspaces");
      if (pkCache instanceof SimpleConcurrentHashMapCacheStorage) removePartitions(pks, pkCache, 0);
      if (pkCache instanceof MemcachedCacheStorage) {
        String totalChunks = String.format("%s|%s", config.getProperty("TILE"), "totalChunks");
        int totalChunk = Integer.parseInt(((String) pkCache.get(totalChunks)).trim());
        // remove each chunk of partition keys
        for (int chunk = 0; chunk < totalChunk; chunk++) {
          removePartitions(pks, pkCache, chunk);
        }
      }
    }
  }

  /**
   * Perform partition key task.
   *
   * @param pkCache the array to be sorted
   */
  @Override
  protected void doPerformTask(CacheStorage pkCache, Utils.CassandraTaskTypes taskName)
      throws IOException, InterruptedException, ExecutionException, TimeoutException {

    String[] pks = metaData.get("partition_key").keySet().toArray(new String[0]);

    List<ImmutablePair<String, String>> ranges = sourceStorageOnCassandra.getTokenRanges();
    int totalRanges = ranges.size();
    List<List<ImmutablePair<String, String>>> tiles = getDistributedRangesByTiles(ranges, Integer.parseInt(config.getProperty("TILES")));
    int currentTile = Integer.parseInt(config.getProperty("TILE"));
    List<ImmutablePair<String, String>> rangeList = tiles.get(currentTile);

    // if tiles = 0 we need to scan one range from one pkScanner, if tiles>0 we need to scan all
    // ranges from the pkScanner
    LOGGER.info("The number of ranges in the cassandra: {}", totalRanges);
    LOGGER.info("The number of ranges for the tile: {}", rangeList.size());
    LOGGER.info("The number of tiles: {}", tiles.size());
    LOGGER.info("The current tile: {}", currentTile);

    scanAndCompare(rangeList, (CacheStorage<String, Long>) pkCache, pks);
    scanAndRemove((CacheStorage<String, Long>) pkCache, pks, taskName);

    StatsMetaData statsMetaDataInserts =
        new StatsMetaData(
            Integer.parseInt(config.getProperty("TILE")),
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"),
            "DELETE");
    statsMetaDataInserts.setValue(statsCounter.getStat("DELETE"));
    targetStorageOnKeyspaces.writeStats(statsMetaDataInserts);
    statsCounter.resetStat("DELETE");
    LOGGER.info("Caching and comparing stage is completed");
    LOGGER.info(
        "The number of pre-loaded elements in the cache is {} ",
        pkCache.getSize(Integer.parseInt(config.getProperty("TILE"))));
  }
}
