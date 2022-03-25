// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.extractor.CassandraExtractor;
import com.amazon.aws.cqlreplicator.extractor.KeyspacesExtractor;
import com.amazon.aws.cqlreplicator.loader.KeyspacesLoader;
import com.amazon.aws.cqlreplicator.models.*;
import com.amazon.aws.cqlreplicator.storage.Storage;
import com.amazon.aws.cqlreplicator.task.AbstractTask;
import com.amazon.aws.cqlreplicator.storage.IonEngine;
import com.amazon.aws.cqlreplicator.util.StatsCounter;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.MapDifference;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.amazon.aws.cqlreplicator.util.Utils.aggregateBuilder;

/** Responsible for replication logic between Cassandra and Amazon Keyspaces */
public class CassandraReplicationTask extends AbstractTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraReplicationTask.class);
  private static final String CLUSTERING_COLUMN_ABSENT = "clusteringColumnAbsent";
  private static final String REPLICATION_NOT_APPLICABLE = "replicationNotApplicable";
  private static final Pattern REGEX_PIPE = Pattern.compile("\\|");

  private static final long MILLISECONDS = 1000000L;
  private static CassandraExtractor cassandraExtractor;
  private static KeyspacesExtractor keyspacesExtractor;
  private static KeyspacesLoader targetLoader;
  private static Map<String, LinkedHashMap<String, String>> cassandraSchemaMetadata;
  private final Properties config;
  private StatsCounter statsCounter;


  public CassandraReplicationTask(Properties config) {
    this.config = config;
    cassandraExtractor = new CassandraExtractor(config);
    targetLoader = new KeyspacesLoader(config);
    cassandraSchemaMetadata = cassandraExtractor.getMetaData();
    keyspacesExtractor = new KeyspacesExtractor(config);
    statsCounter = new StatsCounter();
  }

  private static String transformer(String input, Properties config) {
    if (config.getProperty("TRANSFORM_INBOUND_REQUEST").equals("true")) {
      IonEngine ionEngine = new IonEngine();
      String res = ionEngine.query(config.getProperty("TRANSFORM_SQL"), input);
      return res.substring( 1, res.length() - 1 );
      }
    return input;
  }

  private void replicateCassandraRow(Row row, String[] pks, String[] cls, int timeoutRateLimiter,RateLimiter rateLimiter, Storage pkCache) {

      String[] pk = REGEX_PIPE.split(row.getString("cc"));
      Map<String, Long> ledgerHashMap = new HashMap<>();
      Map<String, Long> sourceHashMap = new HashMap<>();
      Map<String, String> jsonColumnHashMapPerPartition = new HashMap<>();

      BoundStatementBuilder boundStatementCassandraBuilder =
              cassandraExtractor.getCassandraPreparedStatement().boundStatementBuilder();
      int i = 0;
      for (String cl : pks) {
          String type = cassandraSchemaMetadata.get("partition_key").get(cl);
          boundStatementCassandraBuilder =
                  aggregateBuilder(type, cl, pk[i], boundStatementCassandraBuilder);
          i++;
      }
      List<Row> cassandraResult =
              cassandraExtractor.extract(boundStatementCassandraBuilder);

      for (Row compareRow : cassandraResult) {
          String query;
          long ts;
          Payload jsonPayload =
                  Utils.convertToJson(
                          compareRow.getString(0), config.getProperty("WRITETIME_COLUMNS"), cls, pks);
          ts = jsonPayload.getTimestamp();

          Map<String, String> clusteringColumnsMapping = jsonPayload.getClusteringColumns();

          String payload = transformer(jsonPayload.getPayload(), config);

          LOGGER.debug("PAYLOAD: {}", payload);
          // Prepare the JSON CQL statement with "USING TIMESTAMP" from the source
          if (config.getProperty("REPLICATE_WITH_TIMESTAMP").equals("true")) {
              query =
                      String.format(
                              "INSERT INTO %s.%s JSON '%s' USING TIMESTAMP %s",
                              config.getProperty("TARGET_KEYSPACE"),
                              config.getProperty("TARGET_TABLE"),
                              payload,
                              ts);
          } else {
              query =
                      String.format(
                              "INSERT INTO %s.%s JSON '%s'",
                              config.getProperty("TARGET_KEYSPACE"),
                              config.getProperty("TARGET_TABLE"),
                              payload);
          }

          List<String> clTmp = new ArrayList<>();
          for (String cln : cls) {
              if (!cln.equals(CLUSTERING_COLUMN_ABSENT)) {
                  String val = clusteringColumnsMapping.get(cln);
                  clTmp.add(val);
              } else {
                  clTmp.add(REPLICATION_NOT_APPLICABLE);
              }
          }
          String cl = String.join("|", clTmp);
          String hk = String.format("%s|%s", row.getString("cc"), cl);
          // if hk is not in the global pk cache, add it
          try {
              if (!pkCache.containsKey(hk)) {
                  sourceHashMap.put(cl, ts);
                  pkCache.add(Integer.parseInt(config.getProperty("TILE")), hk, ts);
              } else {
                  // if hk is in the global pk cache, compare timestamps
                  if (ts > (long) pkCache.get(hk)) {
                      sourceHashMap.put(cl, ts);
                      pkCache.put(hk, ts);
                  }
                  // if it's less than do nothing
              }
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
              throw new RuntimeException(e);
          }
          jsonColumnHashMapPerPartition.put(cl, query);
      }

      if (sourceHashMap.size() > 0) {
          QueryLedgerItemByPk queryLedgerItemByPk =
                  new QueryLedgerItemByPk(
                          row.getString("cc"),
                          Integer.parseInt(config.getProperty("TILE")),
                          config.getProperty("TARGET_KEYSPACE"),
                          config.getProperty("TARGET_TABLE"));
          List<Row> ledgerResultSet = keyspacesExtractor.extract(queryLedgerItemByPk);

          for (Row ledgerRow : ledgerResultSet) {
              Long lastRun =
                      Objects.requireNonNull(
                              ledgerRow.get("operation_ts", GenericType.ZONED_DATE_TIME))
                              .toEpochSecond()
                              * MILLISECONDS;
              String cl = ledgerRow.getString("cc");
              ledgerHashMap.put(cl, lastRun);
          }
      }

      MapDifference<String, Long> diff = Maps.difference(sourceHashMap, ledgerHashMap);
      Map<String, MapDifference.ValueDifference<Long>> rowDiffering =
              diff.entriesDiffering();
      Map<String, Long> newItems = diff.entriesOnlyOnLeft();

      LOGGER.debug("Started scanning partition: {}", row.getString("cc"));

      // Updating existing Items
      rowDiffering.forEach(
              (k, v) -> {
                  ZonedDateTime valueOnClient = ZonedDateTime.now();
                  if (v.leftValue() > v.rightValue()) {
                      LOGGER.info(
                              "Update a row in the target table {} by partition key: {} and clustering column: {}",
                              config.getProperty("TARGET_TABLE"),
                              row.getString("cc"),
                              k);
                      SimpleStatement simpleStatement =
                              SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                                      .setIdempotent(true)
                                      .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

                      LedgerMetaData ledgerMetaData =
                              new LedgerMetaData(
                                      row.getString("cc"),
                                      k,
                                      config.getProperty("TARGET_KEYSPACE"),
                                      config.getProperty("TARGET_TABLE"),
                                      Integer.parseInt(config.getProperty("TILE")),
                                      valueOnClient,
                                      v.leftValue());
                      StatsMetaData statsMetaData =
                              new StatsMetaData(
                                      Integer.parseInt(config.getProperty("TILE")),
                                      config.getProperty("TARGET_KEYSPACE"),
                                      config.getProperty("TARGET_TABLE"),
                                      "UPDATE");

                      // Update target table
                      RetryEntry retryEntry =
                              new RetryEntry(
                                      row.getString("cc"),
                                      k,
                                      config.getProperty("TARGET_KEYSPACE"),
                                      config.getProperty("TARGET_TABLE"),
                                      "INSERT",
                                      java.time.LocalDate.now());

                      rateLimiter.tryAcquire(1, timeoutRateLimiter, TimeUnit.MILLISECONDS);

                      targetLoader.load(simpleStatement, retryEntry, ledgerMetaData, statsMetaData);

                      statsCounter.incrementStat("UPDATE");

                  }
              });

      // Adding new Items
      newItems.forEach(
              (k, v) -> {
                  LOGGER.debug(
                          "Insert a row into the target table {} by partition key {} and clustering column {}",
                          config.getProperty("TARGET_TABLE"),
                          row.getString("cc"),
                          k);
                  ZonedDateTime valueOnClient = ZonedDateTime.now();
                  SimpleStatement simpleStatement =
                          SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                                  .setIdempotent(true)
                                  .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

                  LedgerMetaData ledgerMetaData =
                          new LedgerMetaData(
                                  row.getString("cc"),
                                  k,
                                  config.getProperty("TARGET_KEYSPACE"),
                                  config.getProperty("TARGET_TABLE"),
                                  Integer.parseInt(config.getProperty("TILE")),
                                  valueOnClient,
                                  v);
                  StatsMetaData statsMetaData =
                          new StatsMetaData(
                                  Integer.parseInt(config.getProperty("TILE")),
                                  config.getProperty("TARGET_KEYSPACE"),
                                  config.getProperty("TARGET_TABLE"),
                                  "INSERT");

                  // Update target table
                  RetryEntry retryEntry =
                          new RetryEntry(
                                  row.getString("cc"),
                                  k,
                                  config.getProperty("TARGET_KEYSPACE"),
                                  config.getProperty("TARGET_TABLE"),
                                  "INSERT",
                                  java.time.LocalDate.now());

                  rateLimiter.tryAcquire(1, timeoutRateLimiter, TimeUnit.MILLISECONDS);

                  targetLoader.load(simpleStatement, retryEntry, ledgerMetaData, statsMetaData);

                  statsCounter.incrementStat("INSERT");

              });
      LOGGER.debug("Completed scanning rows in {}", row.getString("cc"));

  }

  @Override
  protected void doPerformTask(
          Storage pkCache, Utils.CassandraTaskTypes taskName) throws InterruptedException, ExecutionException, TimeoutException {

    int permits = Integer.parseInt(config.getProperty("RATELIMITER_PERMITS"));
    int timeoutRateLimiter = Integer.parseInt(config.getProperty("RATELIMITER_TIMEOUT_MS"));
    RateLimiter rateLimiter = RateLimiter.create(permits);

    PartitionsMetaData partitionsMetaData =
        new PartitionsMetaData(
            Integer.parseInt(config.getProperty("TILE")), config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE"));

    String[] partitionKeyNames = cassandraSchemaMetadata.get("partition_key").keySet().toArray(new String[0]);
    String[] clusteringColumnNames = cassandraSchemaMetadata.get("clustering").keySet().toArray(new String[0]);

    // TODO: Read from the chunked cache instead from the Ledger
    List<Row> ledgerPks = keyspacesExtractor.extract(partitionsMetaData);
    LOGGER.info("The number of pre-loaded elements in the cache is {} ", pkCache.getSize(Integer.parseInt(config.getProperty("TILE"))));

    ledgerPks.parallelStream()
        .forEach(
            row ->
                replicateCassandraRow(row, partitionKeyNames, clusteringColumnNames, timeoutRateLimiter, rateLimiter, pkCache)
            );

    StatsMetaData statsMetaDataInserts =
        new StatsMetaData(
                Integer.parseInt(config.getProperty("TILE")), config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE"), "INSERT");
    statsMetaDataInserts.setValue(statsCounter.getStat("INSERT"));
    targetLoader.load(statsMetaDataInserts);

    StatsMetaData statsMetaDataUpdates =
        new StatsMetaData(
                Integer.parseInt(config.getProperty("TILE")), config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE"), "UPDATE");
      statsMetaDataUpdates.setValue(statsCounter.getStat("UPDATE"));
    targetLoader.load(statsMetaDataUpdates);

    statsCounter.resetStat("INSERT");
    statsCounter.resetStat("UPDATE");
  }
}
