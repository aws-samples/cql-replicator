// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.models.*;
import com.amazon.aws.cqlreplicator.storage.*;
import com.amazon.aws.cqlreplicator.task.AbstractTask;
import com.amazon.aws.cqlreplicator.util.StatsCounter;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

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
  private static final int ADVANCED_CACHE_SIZE = 1000;
  private static SourceStorageOnCassandra sourceStorageOnCassandra;
  private static Map<String, LinkedHashMap<String, String>> metaData;
  private static StatsCounter statsCounter;
  private static LedgerStorageOnLevelDB ledgerStorageOnLevelDB;

  private static TargetStorageOnKeyspaces targetStorageOnKeyspaces;
  private final Properties config;

  /**
   * Constructor for PartitionDiscoveryTask.
   *
   * @param config the array to be sorted
   */
  public PartitionDiscoveryTask(Properties config) throws IOException {
    this.config = config;
    sourceStorageOnCassandra = new SourceStorageOnCassandra(config);
    metaData = sourceStorageOnCassandra.getMetaData();
    statsCounter = new StatsCounter();
    ledgerStorageOnLevelDB = new LedgerStorageOnLevelDB(config);
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
              var totalChunks = String.format("%s|%s", config.getProperty("TILE"), "totalChunks");
              var currentChunk = Integer.parseInt((String) cacheStorage.get(totalChunks));
              LOGGER.debug("{}:{}", totalChunks, currentChunk);
              var cborPayload = Utils.cborEncoder(payload);
              var compressedCborPayload = Utils.compress(cborPayload);
              var keyOfChunk =
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

    var pksStr = String.join(",", pks);

    for (ImmutablePair<String, String> range : rangeList) {
      var rangeStart = Long.parseLong(range.left);
      var rangeEnd = Long.parseLong(range.right);
      //TODO: Make it async
      var resultSetRange =
          sourceStorageOnCassandra.findPartitionsByTokenRange(pksStr, rangeStart, rangeEnd);

      LOGGER.trace("Processing a range: {} - {}", rangeStart, rangeEnd);
      for (Row eachResult : resultSetRange) {
        var i = 0;
        List<String> tmp = new ArrayList<>();

        for (String cl : pks) {
          var type = metaData.get("partition_key").get(cl);
          tmp.add(String.valueOf(eachResult.get(pks[i], Utils.getClassType(type.toUpperCase()))));
          i++;
        }

        var res = String.join("|", tmp);
        var flag = pkCache.containsKey(res);

        if (!flag) {
          pkCache.add(
              Integer.parseInt(config.getProperty("TILE")), res, Instant.now().toEpochMilli());

          var partitionMetaData =
              new PartitionMetaData(
                  Integer.parseInt(config.getProperty("TILE")),
                  config.getProperty("TARGET_KEYSPACE"),
                  config.getProperty("TARGET_TABLE"),
                  res);

          syncPartitionKeys(partitionMetaData);

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

  private void syncPartitionKeys(PartitionMetaData partitionMetaData) {
    ledgerStorageOnLevelDB.writePartitionMetadata(partitionMetaData);
  }

  private void removePartitions(String[] pks, CacheStorage pkCache, int chunk)
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    Collection<?> collection = null;

    if (pkCache instanceof MemcachedCacheStorage) {

      var keyOfChunk = String.format("%s|%s|%s", "pksChunk", config.getProperty("TILE"), chunk);
      var compressedPayload = (byte[]) pkCache.get(keyOfChunk);
      byte[] cborPayload;
      cborPayload = Utils.decompress(compressedPayload);
      collection = Utils.cborDecoder(cborPayload);
    }

    if (pkCache instanceof SimpleConcurrentHashMapCacheStorage) {
      collection = pkCache.keySet();
    }

    Collection<?> finalClonedCollection = new ArrayList<>(collection);
    collection
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

              var i = 0;

              for (String cl : pks) {
                var type = metaData.get("partition_key").get(cl);
                try {
                  boundStatementCassandraBuilder =
                      Utils.aggregateBuilder(type, cl, pk[i], boundStatementCassandraBuilder);
                } catch (Exception e) {
                  LOGGER.error(e.getMessage());
                }
                i++;
              }

              List<Row> cassandraResult = sourceStorageOnCassandra.extract(boundStatementCassandraBuilder);
              
              // Found deleted key
              if (cassandraResult.size() == 0) {
                LOGGER.debug("Found deleted partition key {}", key);

                // Remove partition key from the cache
                if (pkCache instanceof MemcachedCacheStorage) {
                  try {
                    pkCache.remove(Integer.parseInt(config.getProperty("TILE")), (key));
                  } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                  }
                  finalClonedCollection.remove(key);
                }

                if (pkCache instanceof SimpleConcurrentHashMapCacheStorage) {
                  try {
                    pkCache.remove(Integer.parseInt(config.getProperty("TILE")), (key));
                  } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                  }
                }

                // Delete partition from Ledger
                ledgerStorageOnLevelDB.deletePartitionMetadata(new PartitionMetaData(
                        Integer.parseInt(config.getProperty("TILE")),
                        config.getProperty("TARGET_KEYSPACE"),
                        config.getProperty("TARGET_TABLE"),
                        (String) key));

                statsCounter.incrementStat("DELETE");
              }
            });

    if (pkCache instanceof MemcachedCacheStorage
        && finalClonedCollection.size() < collection.size()) {
      var cborPayload = Utils.cborEncoder((List<String>) finalClonedCollection);
      var compressedPayload = Utils.compress(cborPayload);
      var keyOfChunk = String.format("%s|%s|%s", "pksChunk", config.getProperty("TILE"), chunk);
      pkCache.put(keyOfChunk, compressedPayload);
    }

    if (pkCache instanceof MemcachedCacheStorage && finalClonedCollection.size() == 0) {
      var keyOfChunk = String.format("%s|%s", config.getProperty("TILE"), "totalChunks");
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
        var totalChunks = String.format("%s|%s", config.getProperty("TILE"), "totalChunks");
        var totalChunk = Integer.parseInt(((String) pkCache.get(totalChunks)).trim());
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

    var pks = metaData.get("partition_key").keySet().toArray(new String[0]);

    List<ImmutablePair<String, String>> ranges = sourceStorageOnCassandra.getTokenRanges();
    var totalRanges = ranges.size();
    List<List<ImmutablePair<String, String>>> tiles =
        getDistributedRangesByTiles(ranges, Integer.parseInt(config.getProperty("TILES")));
    var currentTile = Integer.parseInt(config.getProperty("TILE"));
    List<ImmutablePair<String, String>> rangeList = tiles.get(currentTile);

    // if tiles = 0 we need to scan one range from one pkScanner, if tiles>0 we need to scan all
    // ranges from the pkScanner
    LOGGER.info("The number of ranges in the cassandra: {}", totalRanges);
    LOGGER.info("The number of ranges for the tile: {}", rangeList.size());
    LOGGER.info("The number of tiles: {}", tiles.size());
    LOGGER.info("The current tile: {}", currentTile);

    scanAndCompare(rangeList, (CacheStorage<String, Long>) pkCache, pks);
    if (config.getProperty("REPLICATE_DELETES").equals("true")) {
      scanAndRemove((CacheStorage<String, Long>) pkCache, pks, taskName);
    }

    var statsMetaDataInserts =
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
