// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.extractor.CassandraExtractor;
import com.amazon.aws.cqlreplicator.extractor.KeyspacesExtractor;
import com.amazon.aws.cqlreplicator.loader.KeyspacesLoader;
import com.amazon.aws.cqlreplicator.models.*;
import com.amazon.aws.cqlreplicator.storage.AdvancedCache;
import com.amazon.aws.cqlreplicator.storage.MemcachedStorage;
import com.amazon.aws.cqlreplicator.storage.SimpleConcurrentHashMapStorage;
import com.amazon.aws.cqlreplicator.storage.Storage;
import com.amazon.aws.cqlreplicator.task.AbstractTask;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.amazon.aws.cqlreplicator.util.Utils.alignRangesAndTiles;

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
  private static CassandraExtractor cassandraExtractor;
  private static KeyspacesExtractor keyspacesExtractor;
  private static KeyspacesLoader targetLoader;
  private static Map<String, LinkedHashMap<String, String>> metaData;
  private final Properties config;
  private static final int REQUEST_PER_PARTITION = 1000;
  private static final int RATE_LIMITER_TIMEOUT_MS = 1000;
  private static final int ADVANCED_CACHE_SIZE = 1000;
  private static final int STATS_HASH_SIZE = 3000;
  private static final float STATS_LOAD_FACTOR = 0.75f;
  private static final int STATS_HASH_CONCUR = 4;

  /**
   * Constructor for PartitionDiscoveryTask.
   *
   * @param config the array to be sorted
   */
  public PartitionDiscoveryTask(Properties config) {
    this.config = config;
    cassandraExtractor = new CassandraExtractor(config);
    keyspacesExtractor = new KeyspacesExtractor(config);
    targetLoader = new KeyspacesLoader(config);
    metaData = cassandraExtractor.getMetaData();
  }

  /** Scan and compare partition keys. */
  private void scanAndCompare(
      List<ImmutablePair<String, String>> rangeList,
      Storage pkCache,
      String[] pks,
      Utils.CassandraTaskTypes taskName) throws IOException, InterruptedException, ExecutionException, TimeoutException {

    AdvancedCache<String> advancedCache = null;

    if (pkCache instanceof MemcachedStorage) {
      advancedCache =
              new AdvancedCache<>(ADVANCED_CACHE_SIZE, pkCache) {
                @Override
                protected void flush(List<String> payload, Storage storage) throws IOException, InterruptedException, ExecutionException, TimeoutException {
                  String totalChunks = String.format("%s|%s", config.getProperty("TILE"), "totalChunks");
                  int currentChunk = Integer.parseInt((String) storage.get(totalChunks));
                  LOGGER.debug("{}:{}", totalChunks, currentChunk);
                    byte[] cborPayload = Utils.cborEncoder(payload);
                    byte[] compressedCborPayload = Utils.compress(cborPayload);
                    String keyOfChunk = String.format("%s|%s|%s", "pksChunk",config.getProperty("TILE"), currentChunk);
                    pkCache.put(keyOfChunk, compressedCborPayload);
                    ((MemcachedStorage) pkCache).incrByOne(totalChunks);
                }
              };
    }

    boolean totalChunksExist = pkCache.containsKey(String.format("%s|%s", config.getProperty("TILE"), "totalChunks"));
    if (!totalChunksExist)
    pkCache.put(String.format("%s|%s", config.getProperty("TILE"), "totalChunks"), "0");

    RateLimiter rateLimiter = RateLimiter.create(REQUEST_PER_PARTITION);
    String pksStr = String.join(",", pks);

    for (ImmutablePair<String, String> range : rangeList) {
      long rangeStart = Long.parseLong(range.left);
      long rangeEnd = Long.parseLong(range.right);

      List<Row> resultSetRange =
          cassandraExtractor.findPartitionsByTokenRange(pksStr, rangeStart,rangeEnd);

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
          pkCache.add(Integer.parseInt(config.getProperty("TILE")), res, Instant.now().toEpochMilli());

          PartitionMetaData partitionMetaData =
                  new PartitionMetaData(
                          Integer.parseInt(config.getProperty("TILE")), config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE"), res);

          RetryEntry retryEntry =
                  new RetryEntry(
                          String.format(
                                  "%s|%s|%s",
                                  Integer.parseInt(config.getProperty("TILE")), config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE")),
                          res,
                          "replicator",
                          "partitionkeys",
                          "INSERT",
                          java.time.LocalDate.now());

          syncPartitionKeys(partitionMetaData, retryEntry, rateLimiter);

          if (advancedCache!=null)
            advancedCache.put(partitionMetaData.getPk());

          LOGGER.debug("Syncing a new partition key: {}", res);
        }
      }
    }

    if (advancedCache!=null && advancedCache.getSize()>0) {
      LOGGER.info("Flushing remainders: {}", advancedCache.getSize());
      advancedCache.doFlush();
    }

    LOGGER.info("Comparing stage is running");
  }

  private void syncPartitionKeys(PartitionMetaData partitionMetaData, RetryEntry retryEntry, RateLimiter rateLimiter) {
      rateLimiter.tryAcquire(1, RATE_LIMITER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      targetLoader.load(retryEntry, partitionMetaData);


  }

  private void removePartitions(String[] pks, Map<String, String> stats, Storage pkCache, int chunk) throws IOException, InterruptedException, ExecutionException, TimeoutException {
    Collection<?> collection = null;


    if (pkCache instanceof MemcachedStorage) {

      String keyOfChunk = String.format("%s|%s|%s", "pksChunk",config.getProperty("TILE"), chunk);
      byte[] compressedPayload = (byte[]) pkCache.get(keyOfChunk);
      byte[] cborPayload;
      cborPayload = Utils.decompress(compressedPayload);
      collection = Utils.cborDecoder(cborPayload);
    }

    if (pkCache instanceof SimpleConcurrentHashMapStorage) {
      collection = pkCache.keySet();
    }

    Collection<?> finalClonedCollection = new ArrayList<>(collection);
    collection.stream()
        .forEach(
            key -> {
              String[] pk;
              BoundStatementBuilder boundStatementCassandraBuilder =
                  cassandraExtractor.getCassandraPreparedStatement().boundStatementBuilder();
              if (pkCache instanceof MemcachedStorage)

                LOGGER.debug("Processing partition key: {}", key);
                pk = REGEX_PIPE.split((String) key);

              if (pkCache instanceof SimpleConcurrentHashMapStorage)
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
                  cassandraExtractor.extract(boundStatementCassandraBuilder);
              // Found deleted key
              if (cassandraResult.size() == 0) {
                LOGGER.debug("Found deleted partition key {}", key);
                // Delete from Ledger

                QueryLedgerItemByPk queryLedgerItemByPk = null;
                PartitionMetaData partitionMetaData = null;

                if (pkCache instanceof MemcachedStorage) {
                  queryLedgerItemByPk =
                      new QueryLedgerItemByPk(
                          (String) key,
                          Integer.parseInt(config.getProperty("TILE")),
                          config.getProperty("TARGET_KEYSPACE"),
                          config.getProperty("TARGET_TABLE"));
                }

                if (pkCache instanceof SimpleConcurrentHashMapStorage) {
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
                if (pkCache instanceof MemcachedStorage) {
                  List<Row> ledgerResultSet = keyspacesExtractor.extract(queryLedgerItemByPk);
                  ledgerResultSet.stream()
                      .forEach(
                          deleteRow ->
                          {
                            try {
                              pkCache.remove(
                                   Integer.parseInt(config.getProperty("TILE")),
                                  "rd",
                                  String.format(
                                      "%s|%s",
                                      ((String) key), deleteRow.getString("cc")));
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
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
                targetLoader.load(queryLedgerItemByPk, partitionMetaData, deleteTargetOperation);
                // Remove partition key from the cache
                if (pkCache instanceof MemcachedStorage) {
                  try {
                    pkCache.remove(Integer.parseInt(config.getProperty("TILE")), ((String) key));
                  } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                  }
                  boolean rs = finalClonedCollection.remove(key);
                }

                if (pkCache instanceof SimpleConcurrentHashMapStorage) {
                  try {
                    pkCache.remove(Integer.parseInt(config.getProperty("TILE")), ((String) key));
                  } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                  }
                }

                stats.put(
                    partitionMetaData.getPk(),
                    String.format("%s|%s", "DELETE", Instant.now().toEpochMilli()));
              }
            });
    if (pkCache instanceof MemcachedStorage && finalClonedCollection.size()<collection.size()) {
        byte[] cborPayload = Utils.cborEncoder((List<String>) finalClonedCollection);
        byte[] compressedPayload = Utils.compress(cborPayload);
        String keyOfChunk = String.format("%s|%s|%s", "pksChunk",config.getProperty("TILE"), chunk);
        pkCache.put(keyOfChunk, compressedPayload);
    }

    if (pkCache instanceof MemcachedStorage && finalClonedCollection.size()==0) {
      String keyOfChunk = String.format("%s|%s", config.getProperty("TILE"), "totalChunks");
      ((MemcachedStorage) pkCache).decrByOne(keyOfChunk);
    }

  }

  /**
   * Scan and remove deleted partition keys.
   *
   * @params rangeList, pkCache, pks the array to be sorted
   */
  private void scanAndRemove(
      Storage pkCache, String[] pks, Utils.CassandraTaskTypes taskName, Map<String, String> stats) throws IOException, InterruptedException, ExecutionException, TimeoutException {

    if (taskName.equals(Utils.CassandraTaskTypes.SYNC_DELETED_PARTITION_KEYS)) {
      LOGGER.info("Syncing deleted partition keys between C* and Amazon Keyspaces");
      if (pkCache instanceof SimpleConcurrentHashMapStorage)
      removePartitions(pks, stats, pkCache, 0);
      if (pkCache instanceof MemcachedStorage) {
        String totalChunks = String.format("%s|%s", config.getProperty("TILE"), "totalChunks");
        int totalChunk = Integer.valueOf((String) pkCache.get(totalChunks));
        // process each chunk of partition keys
        for (int chunk=0; chunk<totalChunk; chunk++) {
          removePartitions(pks, stats, pkCache, chunk);
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
  protected void doPerformTask(
          Storage pkCache, Utils.CassandraTaskTypes taskName) throws IOException, InterruptedException, ExecutionException, TimeoutException {

    Map<String, String> statsCollector = new ConcurrentHashMap<>(STATS_HASH_SIZE, STATS_LOAD_FACTOR, STATS_HASH_CONCUR);
    String[] pks = metaData.get("partition_key").keySet().toArray(new String[0]);

    // Keep all ranges in the format key=range_start and value=range_end
    List<ImmutablePair<String, String>> ranges = cassandraExtractor.getTokenRanges();
    List<ImmutablePair<String, String>> rangeList;

    // Partition the range list by the number of tiles
    int totalRanges = ranges.size();
    LOGGER.debug(String.valueOf(ranges));
    List<List<ImmutablePair<String, String>>> tmpTiles = Lists.partition(ranges, 1);
    List<List<ImmutablePair<String, String>>> res;
    List<List<ImmutablePair<String, String>>> tiles = new ArrayList<>();
    int tmpSize = tmpTiles.size();
    for (int i=0; i<=tmpSize; i++) {
      res = alignRangesAndTiles(tmpTiles);
      tmpTiles = res;
      if (Objects.requireNonNull(tmpTiles).size() == Integer.parseInt(config.getProperty("TILES"))) {
        tiles = tmpTiles;
        break;
      }
    }

    // Get the current tile
    int currentTile = Integer.parseInt(config.getProperty("TILE"));

    if (Integer.parseInt(config.getProperty("TILES")) == 1) {
      // if the number of tiles is 0 let's process all range as the tile 0
      rangeList = ranges;
    } else {
      // Let's process the specific tile
      rangeList = tiles.get(currentTile);
      LOGGER.debug(rangeList.toString());
    }
    // if tiles = 0 we need to scan one range from one pkScanner, if tiles>0 we need to scan all
    // ranges from the pkScanner
    LOGGER.info("The number of ranges in the cassandra: {}", totalRanges);
    LOGGER.info("The number of ranges for the tile: {}", rangeList.size());
    LOGGER.info("The number of tiles: {}", tiles.size());
    LOGGER.info("The current tile: {}", currentTile);

    scanAndCompare(rangeList, (Storage<String, Long>) pkCache, pks, taskName);
    scanAndRemove((Storage<String, Long>) pkCache, pks, taskName, statsCollector);

    // Stats deletes
    long deletes =
            statsCollector.entrySet().stream()
                    .filter(x -> x.getValue().startsWith("DELETE")).count();
    StatsMetaData statsMetaDataInserts =
            new StatsMetaData(
                    Integer.parseInt(config.getProperty("TILE")), config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE"), "DELETE");
    statsMetaDataInserts.setValue(deletes);
    targetLoader.load(statsMetaDataInserts);
    LOGGER.info("Caching and comparing stage is completed");
    LOGGER.info("The number of pre-loaded elements in the cache is {} ", pkCache.getSize(Integer.parseInt(config.getProperty("TILE"))));
  }
}
