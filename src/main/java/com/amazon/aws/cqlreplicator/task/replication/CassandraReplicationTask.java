// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.models.*;
import com.amazon.aws.cqlreplicator.storage.*;
import com.amazon.aws.cqlreplicator.task.AbstractTask;
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
  private static SourceStorageOnCassandra sourceStorageOnCassandra;
  private static TargetStorageOnKeyspaces targetStorageOnKeyspaces;
  private static LedgerStorageOnKeyspaces ledgerStorageOnKeyspaces;
  private static Map<String, LinkedHashMap<String, String>> cassandraSchemaMetadata;
  private final Properties config;
  private static StatsCounter statsCounter;

  public CassandraReplicationTask(Properties config) {
    this.config = config;
    sourceStorageOnCassandra = new SourceStorageOnCassandra(config);
    cassandraSchemaMetadata = sourceStorageOnCassandra.getMetaData();
    statsCounter = new StatsCounter();
    targetStorageOnKeyspaces = new TargetStorageOnKeyspaces(config);
    ledgerStorageOnKeyspaces = new LedgerStorageOnKeyspaces(config);
  }

  private static String transformer(String input, Properties config) {
    if (config.getProperty("TRANSFORM_INBOUND_REQUEST").equals("true")) {
      IonEngine ionEngine = new IonEngine();
      String res = ionEngine.query(config.getProperty("TRANSFORM_SQL"), input);
      return res.substring(1, res.length() - 1);
    }
    return input;
  }

  private void dataLoader(
      SimpleStatement simpleStatement,
      LedgerMetaData ledgerMetaData,
      boolean setStartReplicationPoint,
      long ts) {
    if (!setStartReplicationPoint) {
      targetStorageOnKeyspaces.write(simpleStatement);
      ledgerStorageOnKeyspaces.writeRowMetadata(ledgerMetaData);
    } else {
      if (ledgerMetaData.getLastWriteTime() > ts) {
        targetStorageOnKeyspaces.write(simpleStatement);
        ledgerStorageOnKeyspaces.writeRowMetadata(ledgerMetaData);
      }
    }
  }

  private void replicateCassandraRow(
      Row row,
      String[] pks,
      String[] cls,
      int timeoutRateLimiter,
      RateLimiter rateLimiter,
      CacheStorage pkCache) {

    String[] pk = REGEX_PIPE.split(row.getString("cc"));
    Map<String, Long> ledgerHashMap = new HashMap<>();
    Map<String, Long> sourceHashMap = new HashMap<>();
    Map<String, String> jsonColumnHashMapPerPartition = new HashMap<>();

    BoundStatementBuilder boundStatementCassandraBuilder =
        sourceStorageOnCassandra.getCassandraPreparedStatement().boundStatementBuilder();

    int i = 0;
    for (String cl : pks) {
      String type = cassandraSchemaMetadata.get("partition_key").get(cl);
      boundStatementCassandraBuilder =
          aggregateBuilder(type, cl, pk[i], boundStatementCassandraBuilder);
      i++;
    }

    List<Row> cassandraResult = sourceStorageOnCassandra.extract(boundStatementCassandraBuilder);

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
                config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE"), payload);
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

      List<Row> ledgerResultSet = ledgerStorageOnKeyspaces.readRowMetaData(queryLedgerItemByPk);

      ledgerResultSet.parallelStream()
          .forEach(
              ledgerRow -> {
                Long lastRun =
                    Objects.requireNonNull(
                                ledgerRow.get("operation_ts", GenericType.ZONED_DATE_TIME))
                            .toEpochSecond()
                        * MILLISECONDS;
                String cl = ledgerRow.getString("cc");
                ledgerHashMap.put(cl, lastRun);
              });
    }

    MapDifference<String, Long> diff = Maps.difference(sourceHashMap, ledgerHashMap);
    Map<String, MapDifference.ValueDifference<Long>> rowDiffering = diff.entriesDiffering();
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

            rateLimiter.tryAcquire(1, timeoutRateLimiter, TimeUnit.MILLISECONDS);

            dataLoader(
                simpleStatement,
                ledgerMetaData,
                Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
                Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")));
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

          rateLimiter.tryAcquire(1, timeoutRateLimiter, TimeUnit.MILLISECONDS);

          dataLoader(
              simpleStatement,
              ledgerMetaData,
              Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
              Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")));
          statsCounter.incrementStat("INSERT");

        });
    LOGGER.debug("Completed scanning rows in {}", row.getString("cc"));
  }

  @Override
  protected void doPerformTask(CacheStorage pkCache, Utils.CassandraTaskTypes taskName)
      throws InterruptedException, ExecutionException, TimeoutException {

    int permits = Integer.parseInt(config.getProperty("RATELIMITER_PERMITS"));
    int timeoutRateLimiter = Integer.parseInt(config.getProperty("RATELIMITER_TIMEOUT_MS"));
    RateLimiter rateLimiter = RateLimiter.create(permits);

    PartitionsMetaData partitionsMetaData =
        new PartitionsMetaData(
            Integer.parseInt(config.getProperty("TILE")),
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"));

    String[] partitionKeyNames =
        cassandraSchemaMetadata.get("partition_key").keySet().toArray(new String[0]);
    String[] clusteringColumnNames =
        cassandraSchemaMetadata.get("clustering").keySet().toArray(new String[0]);

    //List<Row> ledgerPks = keyspacesExtractor.extract(partitionsMetaData);
    List<Row> ledgerPks = ledgerStorageOnKeyspaces.readPartitionsMetadata(partitionsMetaData);
    LOGGER.info(
        "The number of pre-loaded elements in the cache is {} ",
        pkCache.getSize(Integer.parseInt(config.getProperty("TILE"))));

    ledgerPks.parallelStream()
        .forEach(
            row ->
                replicateCassandraRow(
                    row,
                    partitionKeyNames,
                    clusteringColumnNames,
                    timeoutRateLimiter,
                    rateLimiter,
                    pkCache));

    StatsMetaData statsMetaDataInserts =
        new StatsMetaData(
            Integer.parseInt(config.getProperty("TILE")),
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"),
            "INSERT");

    statsMetaDataInserts.setValue(statsCounter.getStat("INSERT"));
    //targetLoader.load(statsMetaDataInserts);
    if (statsMetaDataInserts.getValue()>0) {
    targetStorageOnKeyspaces.writeStats(statsMetaDataInserts);
    }

    StatsMetaData statsMetaDataUpdates =
        new StatsMetaData(
            Integer.parseInt(config.getProperty("TILE")),
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"),
            "UPDATE");

    statsMetaDataUpdates.setValue(statsCounter.getStat("UPDATE"));

    if (statsMetaDataUpdates.getValue() > 0) {
      targetStorageOnKeyspaces.writeStats(statsMetaDataUpdates);
    }

    statsCounter.resetStat("INSERT");
    statsCounter.resetStat("UPDATE");
  }
}
