// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.models.*;
import com.amazon.aws.cqlreplicator.storage.*;
import com.amazon.aws.cqlreplicator.task.AbstractTask;
import com.amazon.aws.cqlreplicator.util.CustomResultSetSerializer;
import com.amazon.aws.cqlreplicator.util.StatsCounter;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.shaded.guava.common.collect.MapDifference;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.amazon.aws.cqlreplicator.util.Utils.*;

/** Responsible for replication logic between Cassandra and Amazon Keyspaces */
public class CassandraReplicationTask extends AbstractTask {

  public static final String CLUSTERING_COLUMN_ABSENT = "clusteringColumnAbsent";
  public static final String REPLICATION_NOT_APPLICABLE = "replicationNotApplicable";
  public static final Pattern REGEX_PIPE = Pattern.compile("\\|");
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraReplicationTask.class);
  private static final int BLOCKING_QUEUE_SIZE = 15000;
  private static SourceStorageOnCassandra sourceStorageOnCassandra;
  private static TargetStorageOnKeyspaces targetStorageOnKeyspaces;
  private static LedgerStorageOnLevelDB ledgerStorageOnLevelDB;
  private static Map<String, LinkedHashMap<String, String>> cassandraSchemaMetadata;
  private static StatsCounter statsCounter;
  private static Properties config = new Properties();
  private static int CORE_POOL_SIZE;
  private static int MAX_CORE_POOL_SIZE;
  private static int CORE_POOL_TIMEOUT;
  private static CloudWatchClient cloudWatchClient;
  private static boolean useCustomJsonSerializer = false;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final SimpleModule module = new SimpleModule();

  public CassandraReplicationTask(final Properties cfg) throws IOException {
    config = cfg;
    CORE_POOL_SIZE = Integer.parseInt(cfg.getProperty("REPLICATE_WITH_CORE_POOL_SIZE"));
    MAX_CORE_POOL_SIZE = Integer.parseInt(cfg.getProperty("REPLICATE_WITH_MAX_CORE_POOL_SIZE"));
    CORE_POOL_TIMEOUT = Integer.parseInt(cfg.getProperty("REPLICATE_WITH_CORE_POOL_TIMEOUT"));
    sourceStorageOnCassandra = new SourceStorageOnCassandra(config);
    cassandraSchemaMetadata = sourceStorageOnCassandra.getMetaData();
    statsCounter = new StatsCounter();
    targetStorageOnKeyspaces = new TargetStorageOnKeyspaces(config);
    ledgerStorageOnLevelDB = new LedgerStorageOnLevelDB(config);
    useCustomJsonSerializer = !cfg.getProperty("SOURCE_CQL_QUERY").split(" ")[1].toLowerCase().equals("json");
    if (useCustomJsonSerializer) {
      module.addSerializer(Row.class, new CustomResultSetSerializer());
      mapper.registerModule(module);
    }

    if (config.getProperty("ENABLE_CLOUD_WATCH").equals("true")) {
      try {
        cloudWatchClient =
            CloudWatchClient.builder()
                .region(Region.of(config.getProperty("CLOUD_WATCH_REGION")))
                .build();
      } catch (CloudWatchException e) {
        throw new RuntimeException(e);
      }
      }
  }

  protected static String transformer(final String input, final Properties config) {
    if (config.getProperty("TRANSFORM_INBOUND_REQUEST").equals("true")) {
      var ionEngine = new IonEngine();
      var res = ionEngine.query(config.getProperty("TRANSFORM_SQL"), input);
      return res.substring(1, res.length() - 1);
    }
    return input;
  }

  private static String preparePayload(final Payload jsonPayload) {
    var ts = jsonPayload.getTimestamp();
    var query = "";
    var payload = transformer(jsonPayload.getPayload(), config);

    LOGGER.debug("PAYLOAD: {}", payload);
    // Prepare the JSON CQL statement with "USING TIMESTAMP" from the source
    if (config.getProperty("REPLICATE_WITH_TIMESTAMP").equals("true")) {
      query =
          String.format(
              doubleQuoteResolver("INSERT INTO %s.%s JSON '%s' USING TIMESTAMP %s", config.getProperty("SOURCE_CQL_QUERY")),
              config.getProperty("TARGET_KEYSPACE"),
              config.getProperty("TARGET_TABLE"),
              payload,
              ts);
    } else {
      query =
          String.format(
              doubleQuoteResolver("INSERT INTO %s.%s JSON '%s'", config.getProperty("SOURCE_CQL_QUERY")),
              config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE"), payload);
    }
    return query;
  }

  private void delete(
      final PrimaryKey primaryKey,
      final String[] pks,
      final String[] cls,
      CacheStorage<String, String> pkCache)
      throws IOException, InterruptedException, ExecutionException, TimeoutException, ArrayIndexOutOfBoundsException {
    if (!sourceStorageOnCassandra.findPrimaryKey(primaryKey, pks, cls)) {
      var rowIsDeleted =
          targetStorageOnKeyspaces.delete(primaryKey, pks, cls, cassandraSchemaMetadata);
      if (rowIsDeleted) {
        ledgerStorageOnLevelDB.deleteRowMetadata(
            new LedgerMetaData(
                primaryKey.getPartitionKeys(),
                primaryKey.getClusteringColumns(),
                // TODO: Remove overhead params
                config.getProperty("TARGET_KEYSPACE"),
                config.getProperty("TARGET_TABLE"),
                Integer.parseInt(config.getProperty("TILE")),
                0,
                0));
        pkCache.remove(
            Integer.parseInt(config.getProperty("TILE")),
            "rd",
            String.format(
                "%s|%s", primaryKey.getPartitionKeys(), primaryKey.getClusteringColumns()));
        statsCounter.incrementStat("DELETE");
      }
    }
  }

  private void replicateDeletedCassandraRow(
      final String[] pks, final String[] cls, CacheStorage<String, String> pkCache) {

    var ledger = ledgerStorageOnLevelDB.readPaginatedPrimaryKeys();
    ledger.forEachRemaining(
        primaryKeys ->
            primaryKeys.parallelStream()
                .forEach(
                    pk -> {
                      try {
                        delete(pk, pks, cls, pkCache);
                      } catch (IOException
                          | ArrayIndexOutOfBoundsException
                          | InterruptedException
                          | ExecutionException
                          | TimeoutException e) {
                        if (e instanceof ArrayIndexOutOfBoundsException) {
                          LOGGER.error("Perhaps one of the columns in your primary key is empty! pks:{}, cls:{}, pk:{}", Arrays.toString(pks), Arrays.toString(cls), pk);
                        }
                        throw new RuntimeException(e);
                      }
                    }));
  }

  private static void persistMetrics(StatsMetaData statsMetadata){
    if (statsMetadata.getValue() > 0) {
      if (config.getProperty("ENABLE_CLOUD_WATCH").equals("false")) {
        targetStorageOnKeyspaces.writeStats(statsMetadata);
      } else {
        var metricVal = statsMetadata.getValue();
        var metricName = statsMetadata.getOps();
        putMetricData(cloudWatchClient, Double.valueOf(String.valueOf(metricVal)), metricName);
      }
    }
  }

  private static String getSerializedCassandraRow(Row row) throws JsonProcessingException {
    String payload;
    if (useCustomJsonSerializer) {
      payload = mapper.writeValueAsString(row).replace("\\\"writetime(", "writetime(").replace(")\\\"",")");
    } else
    {
      payload = row.getString(0);
    }
    return payload;
  }

  @Override
  protected void doPerformTask(CacheStorage pkCache, Utils.CassandraTaskTypes taskName)
      throws InterruptedException, ExecutionException, TimeoutException, IOException {

    var partitionKeyNames =
        cassandraSchemaMetadata.get("partition_key").keySet().toArray(new String[0]);
    var clusteringColumnNames =
        cassandraSchemaMetadata.get("clustering").keySet().toArray(new String[0]);

    BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_SIZE);

    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            CORE_POOL_SIZE,
            MAX_CORE_POOL_SIZE,
            CORE_POOL_TIMEOUT,
            TimeUnit.SECONDS,
            blockingQueue,
            new ThreadPoolExecutor.AbortPolicy());

    // Let's get all available partitions by chunks

    LOGGER.info(
        "The number of pre-loaded elements in the cache is {} ",
        pkCache.getSize(Integer.parseInt(config.getProperty("TILE"))));

    var chunks =
        ((MemcachedCacheStorage) pkCache)
            .getTotalChunks(Integer.parseInt(config.getProperty("TILE")));

    if (chunks > 0) {
      if (config.getProperty("ENABLE_INTERNAL_PARTITION_KEY_STORAGE").equals("false")) {

        var stream = IntStream.range(0, chunks);
        stream
            .parallel()
            .forEach(
                chunk -> {
                  List<Object> listOfPartitionKeys;
                  try {
                    listOfPartitionKeys =
                        ((MemcachedCacheStorage) pkCache)
                            .getListOfPartitionKeysByChunk(
                                chunk, Integer.parseInt(config.getProperty("TILE")));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  assert listOfPartitionKeys != null;

                  executor.prestartAllCoreThreads();
                  listOfPartitionKeys.forEach(
                      row ->
                          blockingQueue.offer(
                              new RowReplicationTask(
                                  partitionKeyNames,
                                  clusteringColumnNames,
                                  new PrimaryKey(row.toString(), config.getProperty("TILE")),
                                  pkCache)));
                });

      } else {

        Stream<Object> ledgerPks =
            StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(
                        ledgerStorageOnLevelDB.readPaginatedPartitionsMetadata(), 0),
                    true)
                .flatMap(List::stream);

        LOGGER.info(
            "The number of pre-loaded elements in the cache is {} ",
            pkCache.getSize(Integer.parseInt(config.getProperty("TILE"))));

        executor.prestartAllCoreThreads();
        ledgerPks.forEach(
            row ->
                blockingQueue.offer(
                    new RowReplicationTask(
                        partitionKeyNames,
                        clusteringColumnNames,
                        new PrimaryKey(row.toString(), config.getProperty("TILE")),
                        pkCache)));
      }
    }

    if (config.getProperty("REPLICATE_DELETES").equals("true")) {
      replicateDeletedCassandraRow(partitionKeyNames, clusteringColumnNames, pkCache);
    }

    var statsMetaDataInserts =
        new StatsMetaData(
            Integer.parseInt(config.getProperty("TILE")),
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"),
            "INSERT");

    statsMetaDataInserts.setValue(statsCounter.getStat("INSERT"));
    persistMetrics(statsMetaDataInserts);

    var statsMetaDataUpdates =
        new StatsMetaData(
            Integer.parseInt(config.getProperty("TILE")),
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"),
            "UPDATE");

    statsMetaDataUpdates.setValue(statsCounter.getStat("UPDATE"));
    persistMetrics(statsMetaDataUpdates);

    var statsMetaDataDeletes =
        new StatsMetaData(
            Integer.parseInt(config.getProperty("TILE")),
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"),
            "DELETE");

    statsMetaDataDeletes.setValue(statsCounter.getStat("DELETE"));
    persistMetrics(statsMetaDataDeletes);

    statsCounter.resetStat("INSERT");
    statsCounter.resetStat("UPDATE");
    statsCounter.resetStat("DELETE");

    executor.shutdown();
    executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  public static class RowReplicationTask implements Runnable {

    private static CacheStorage<String, Long> pkCache;
    private final String[] pks;
    private final String[] cls;
    private final PrimaryKey primaryKey;

    public RowReplicationTask(
        final String[] pks,
        final String[] cls,
        final PrimaryKey pk,
        CacheStorage<String, Long> pkc) {
      this.pks = pks;
      this.cls = cls;
      this.primaryKey = pk;
      pkCache = pkc;
    }

    private void dataLoader(
        final SimpleStatement simpleStatement,
        final LedgerMetaData ledgerMetaData,
        final boolean setStartReplicationPoint,
        final long ts,
        final String ops)
        throws IOException {
      if (!setStartReplicationPoint) {
        var result = targetStorageOnKeyspaces.write(simpleStatement);
        if (result) {
          ledgerStorageOnLevelDB.writeRowMetadata(ledgerMetaData);
          statsCounter.incrementStat(ops);
        }

      } else {
        if (ledgerMetaData.getLastWriteTime() > ts) {
          var result = targetStorageOnKeyspaces.write(simpleStatement);
          if (result) {
            ledgerStorageOnLevelDB.writeRowMetadata(ledgerMetaData);
            statsCounter.incrementStat(ops);
          }
        }
      }
    }

    private void insertRow(
        final long v,
        final String k,
        final ConcurrentMap<String, String> jsonColumnHashMapPerPartition,
        final PrimaryKey primaryKey)
        throws IOException {
      var valueOnClient = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
      var simpleStatement =
          SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
              .setIdempotent(true)
              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

      var ledgerMetaData =
          new LedgerMetaData(
              primaryKey.getPartitionKeys(),
              k,
              config.getProperty("TARGET_KEYSPACE"),
              config.getProperty("TARGET_TABLE"),
              Integer.parseInt(config.getProperty("TILE")),
              valueOnClient,
              v);
      dataLoader(
          simpleStatement,
          ledgerMetaData,
          Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
          Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")),
          "INSERT");
    }

    private void updateRow(
        final MapDifference.ValueDifference<Long> v,
        final String k,
        final ConcurrentMap<String, String> jsonColumnHashMapPerPartition,
        final PrimaryKey primaryKey)
        throws IOException {
      var valueOnClient = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
      if (v.leftValue() > v.rightValue()) {
        var simpleStatement =
            SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                .setIdempotent(true)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        var ledgerMetaData =
            new LedgerMetaData(
                primaryKey.getPartitionKeys(),
                k,
                config.getProperty("TARGET_KEYSPACE"),
                config.getProperty("TARGET_TABLE"),
                Integer.parseInt(config.getProperty("TILE")),
                valueOnClient,
                v.leftValue());
        dataLoader(
            simpleStatement,
            ledgerMetaData,
            Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
            Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")),
            "UPDATE");
      }
    }

    public static BoundStatementBuilder prepareCassandraStatement(String[] pk, String[] pks) {
      BoundStatementBuilder boundStatementCassandraBuilder =
              sourceStorageOnCassandra.getCassandraPreparedStatement().boundStatementBuilder();
      int i = 0;
      try {
        for (String columnName : pks) {
          var type = cassandraSchemaMetadata.get("partition_key").get(columnName);
          boundStatementCassandraBuilder =
                  aggregateBuilder(type, columnName, pk[i], boundStatementCassandraBuilder);
          i++;
        }
      } catch (ArrayIndexOutOfBoundsException exception) {
        LOGGER.error("Incorrect pk {} in a position {}", Arrays.toString(pk), i);
        LOGGER.error("Reason: perhaps one of the column in the primary key is an empty string");
        throw new RuntimeException(exception);
      }
      return boundStatementCassandraBuilder;
    }

    @Override
    public void run() {

      var pk = REGEX_PIPE.split(primaryKey.getPartitionKeys());
      ConcurrentMap<String, Long> ledgerHashMap = new ConcurrentHashMap<>();
      ConcurrentMap<String, Long> sourceHashMap = new ConcurrentHashMap<>();
      ConcurrentMap<String, String> jsonColumnHashMapPerPartition = new ConcurrentHashMap<>();

      var boundStatementCassandraBuilder = prepareCassandraStatement(pk, pks);

      var cassandraResult = sourceStorageOnCassandra.extract(boundStatementCassandraBuilder);
      cassandraResult.parallelStream()
          .forEach(
              compareRow -> {
                Payload jsonPayload = null;
                try {
                  jsonPayload = Utils.convertToJson(
                          getSerializedCassandraRow(compareRow),
                          config.getProperty("WRITETIME_COLUMNS"),
                          cls,
                          pks);
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
                var ts = jsonPayload.getTimestamp();
                Map<String, String> clusteringColumnsMapping = jsonPayload.getClusteringColumns();
                List<String> clTmp = new ArrayList<>();
                for (String cln : cls) {
                  if (!cln.equals(CLUSTERING_COLUMN_ABSENT)) {
                    var val = clusteringColumnsMapping.get(cln);
                    clTmp.add(val);
                  } else {
                    clTmp.add(REPLICATION_NOT_APPLICABLE);
                  }
                }
                var cl = String.join("|", clTmp);
                var hk = String.format("%s|%s", primaryKey.getPartitionKeys(), cl);
                // if hk is not in the global pk cache, add it
                try {
                  if (!pkCache.containsKey(hk)) {
                    sourceHashMap.put(cl, ts);
                    pkCache.add(Integer.parseInt(config.getProperty("TILE")), hk, ts);
                    jsonColumnHashMapPerPartition.put(cl, preparePayload(jsonPayload));
                  } else {
                    // if hk is in the global pk cache, compare timestamps
                    if (ts > (long) pkCache.get(hk)) {
                      sourceHashMap.put(cl, ts);
                      pkCache.put(hk, ts);
                      var query = preparePayload(jsonPayload);
                      jsonColumnHashMapPerPartition.put(cl, preparePayload(jsonPayload));
                    }
                    // if it's less than do nothing
                  }
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                  throw new RuntimeException(e);
                }
              });

      if (!sourceHashMap.isEmpty()) {
        QueryLedgerItemByPk queryLedgerItemByPk =
            new QueryLedgerItemByPk(
                primaryKey.getPartitionKeys(),
                Integer.parseInt(config.getProperty("TILE")),
                config.getProperty("TARGET_KEYSPACE"),
                config.getProperty("TARGET_TABLE"));

        List<Object> ledgerResultSet;
        try {
          ledgerResultSet = ledgerStorageOnLevelDB.readRowMetaData(queryLedgerItemByPk);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        if (!ledgerResultSet.isEmpty()) {
          ledgerResultSet.parallelStream()
              .forEach(
                  ledgerRow -> {
                    var r = (Value) ledgerRow;
                    var lastRun = r.getLastRun();
                    var cl = r.getCk();
                    ledgerHashMap.put(cl, lastRun);
                  });
        }
      }

      MapDifference<String, Long> diff = Maps.difference(sourceHashMap, ledgerHashMap);
      Map<String, MapDifference.ValueDifference<Long>> rowDiffering = diff.entriesDiffering();
      Map<String, Long> newItems = diff.entriesOnlyOnLeft();

      rowDiffering.forEach(
          (k, v) -> {
            try {
              updateRow(v, k, jsonColumnHashMapPerPartition, primaryKey);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });

      newItems.forEach(
          (k, v) -> {
            try {
              insertRow(v, k, jsonColumnHashMapPerPartition, primaryKey);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }
}
