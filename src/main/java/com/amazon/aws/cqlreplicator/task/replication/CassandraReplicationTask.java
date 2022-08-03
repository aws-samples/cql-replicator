// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.models.LedgerMetaData;
import com.amazon.aws.cqlreplicator.models.PartitionsMetaData;
import com.amazon.aws.cqlreplicator.models.QueryLedgerItemByPk;
import com.amazon.aws.cqlreplicator.models.StatsMetaData;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
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
  private static StatsCounter statsCounter;
  private final Properties config;

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
      var ionEngine = new IonEngine();
      var res = ionEngine.query(config.getProperty("TRANSFORM_SQL"), input);
      return res.substring(1, res.length() - 1);
    }
    return input;
  }

  private void dataLoader(
      SimpleStatement simpleStatement,
      LedgerMetaData ledgerMetaData,
      boolean setStartReplicationPoint,
      long ts, String ops) {
    if (!setStartReplicationPoint) {
      targetStorageOnKeyspaces.write(simpleStatement);
      ledgerStorageOnKeyspaces.writeRowMetadata(ledgerMetaData);
      statsCounter.incrementStat(ops);

    } else {
      if (ledgerMetaData.getLastWriteTime() > ts) {
        targetStorageOnKeyspaces.write(simpleStatement);
        ledgerStorageOnKeyspaces.writeRowMetadata(ledgerMetaData);
        statsCounter.incrementStat(ops);
      }
    }
  }

  private void replicateCassandraRow(Row row, String[] pks, String[] cls, CacheStorage pkCache) {

    var pk = REGEX_PIPE.split(row.getString("cc"));
    Map<String, Long> ledgerHashMap = new HashMap<>();
    Map<String, Long> sourceHashMap = new HashMap<>();
    Map<String, String> jsonColumnHashMapPerPartition = new HashMap<>();

    BoundStatementBuilder boundStatementCassandraBuilder =
        sourceStorageOnCassandra.getCassandraPreparedStatement().boundStatementBuilder();

    int i = 0;
    for (String cl : pks) {
      var type = cassandraSchemaMetadata.get("partition_key").get(cl);
      boundStatementCassandraBuilder =
          aggregateBuilder(type, cl, pk[i], boundStatementCassandraBuilder);
      i++;
    }
    // TODO: Move to async paging
    var cassandraResult = sourceStorageOnCassandra.extract(boundStatementCassandraBuilder);

    for (Row compareRow : cassandraResult) {
      String query;
      long ts;
      var jsonPayload =
          Utils.convertToJson(
              compareRow.getString(0), config.getProperty("WRITETIME_COLUMNS"), cls, pks);
      ts = jsonPayload.getTimestamp();

      Map<String, String> clusteringColumnsMapping = jsonPayload.getClusteringColumns();

      var payload = transformer(jsonPayload.getPayload(), config);

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
          var val = clusteringColumnsMapping.get(cln);
          clTmp.add(val);
        } else {
          clTmp.add(REPLICATION_NOT_APPLICABLE);
        }
      }
      var cl = String.join("|", clTmp);
      var hk = String.format("%s|%s", row.getString("cc"), cl);
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
            var simpleStatement =
                SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                    .setIdempotent(true)
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

            var ledgerMetaData =
                new LedgerMetaData(
                    row.getString("cc"),
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
        });

    // Adding new Items
    newItems.forEach(
        (k, v) -> {
          LOGGER.debug(
              "Insert a row into the target table {} by partition key {} and clustering column {}",
              config.getProperty("TARGET_TABLE"),
              row.getString("cc"),
              k);
          var valueOnClient = ZonedDateTime.now();
          var simpleStatement =
              SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                  .setIdempotent(true)
                  .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

          var ledgerMetaData =
              new LedgerMetaData(
                  row.getString("cc"),
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
        });
    LOGGER.debug("Completed scanning rows in {}", row.getString("cc"));
  }

  @Override
  protected void doPerformTask(CacheStorage pkCache, Utils.CassandraTaskTypes taskName)
      throws InterruptedException, ExecutionException, TimeoutException {

    var partitionsMetaData =
        new PartitionsMetaData(
            Integer.parseInt(config.getProperty("TILE")),
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"));

    var partitionKeyNames =
        cassandraSchemaMetadata.get("partition_key").keySet().toArray(new String[0]);
    var clusteringColumnNames =
        cassandraSchemaMetadata.get("clustering").keySet().toArray(new String[0]);

    var ledgerPks = ledgerStorageOnKeyspaces.readPartitionsMetadata(partitionsMetaData);
    LOGGER.info(
        "The number of pre-loaded elements in the cache is {} ",
        pkCache.getSize(Integer.parseInt(config.getProperty("TILE"))));

    ledgerPks.parallelStream()
        .forEach(
            row -> replicateCassandraRow(row, partitionKeyNames, clusteringColumnNames, pkCache));

    var statsMetaDataInserts =
        new StatsMetaData(
            Integer.parseInt(config.getProperty("TILE")),
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"),
            "INSERT");

    statsMetaDataInserts.setValue(statsCounter.getStat("INSERT"));
    if (statsMetaDataInserts.getValue() > 0) {
      targetStorageOnKeyspaces.writeStats(statsMetaDataInserts);
    }

    var statsMetaDataUpdates =
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
