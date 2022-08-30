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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import com.datastax.oss.driver.shaded.guava.common.collect.MapDifference;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.amazon.aws.cqlreplicator.util.Utils.aggregateBuilder;

/** Responsible for replication logic between Cassandra and Amazon Keyspaces */
public class CassandraReplicationTask extends AbstractTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraReplicationTask.class);
  private static final String CLUSTERING_COLUMN_ABSENT = "clusteringColumnAbsent";
  private static final String REPLICATION_NOT_APPLICABLE = "replicationNotApplicable";
  private static final Pattern REGEX_PIPE = Pattern.compile("\\|");

  private static SourceStorageOnCassandra sourceStorageOnCassandra;
  private static TargetStorageOnKeyspaces targetStorageOnKeyspaces;
  private static LedgerStorageOnLevelDB ledgerStorageOnLevelDB;
  private static Map<String, LinkedHashMap<String, String>> cassandraSchemaMetadata;
  private static StatsCounter statsCounter;
  private final Properties config;

  public CassandraReplicationTask(Properties config) throws IOException {
    this.config = config;
    sourceStorageOnCassandra = new SourceStorageOnCassandra(config);
    cassandraSchemaMetadata = sourceStorageOnCassandra.getMetaData();
    statsCounter = new StatsCounter();
    targetStorageOnKeyspaces = new TargetStorageOnKeyspaces(config);
    ledgerStorageOnLevelDB = new LedgerStorageOnLevelDB(config);
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
      long ts, String ops) throws IOException{
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


  private void replicateDeletedCassandraRow(String[] pks, String[] cls, CacheStorage<String, String> pkCache) {
    var methodName = new Throwable().getStackTrace()[0].getMethodName();
    var startTime = System.nanoTime();

    var ledger = ledgerStorageOnLevelDB.readPaginatedPrimaryKeys();
    while (ledger.hasNext()) {
      var primaryKeys = ledger.next();
      primaryKeys.stream()
          .parallel()
          .filter(primaryKey -> (!sourceStorageOnCassandra.findPrimaryKey(primaryKey, pks, cls)))
          .forEach(primaryKey->{
            targetStorageOnKeyspaces.delete(primaryKey,pks, cls,cassandraSchemaMetadata);
            try {
              ledgerStorageOnLevelDB.deleteRowMetadata(new LedgerMetaData(primaryKey.getPartitionKeys(),
                      primaryKey.getClusteringColumns(),
                      // TODO: Remove overhead params
                      config.getProperty("TARGET_KEYSPACE"),
                      config.getProperty("TARGET_TABLE"),
                      Integer.parseInt(config.getProperty("TILE")), 0, 0
                      ));
              pkCache.remove(
                      Integer.parseInt(config.getProperty("TILE")),
                      "rd",
                      String.format(
                              "%s|%s", primaryKey.getPartitionKeys(), primaryKey.getClusteringColumns()));
              statsCounter.incrementStat("DELETE");
            } catch (InterruptedException | ExecutionException | TimeoutException | IOException e)  {
              throw new RuntimeException();
            }
          });
      }

    var elapsedTime = System.nanoTime() - startTime;
    LOGGER.debug("Call {} : {} ms", methodName, Duration.ofNanos(elapsedTime).toMillis());
    }


  private void replicateCassandraRow(PrimaryKey row, String[] pks, String[] cls, CacheStorage pkCache) throws IOException {

    var pk = REGEX_PIPE.split(row.getPartitionKeys());
    ConcurrentMap<String, Long> ledgerHashMap = new ConcurrentHashMap<>();
    ConcurrentMap<String, Long> sourceHashMap = new ConcurrentHashMap<>();
    ConcurrentMap<String, String> jsonColumnHashMapPerPartition = new ConcurrentHashMap<>();

    BoundStatementBuilder boundStatementCassandraBuilder =
        sourceStorageOnCassandra.getCassandraPreparedStatement().boundStatementBuilder();

    int i = 0;
    for (String columnName : pks) {
      var type = cassandraSchemaMetadata.get("partition_key").get(columnName);
      boundStatementCassandraBuilder =
          aggregateBuilder(type, columnName, pk[i], boundStatementCassandraBuilder);
      i++;
    }

    var cassandraResult = sourceStorageOnCassandra.extract(boundStatementCassandraBuilder);

    //for (Row compareRow : cassandraResult) {
    cassandraResult.parallelStream().forEach(compareRow->{
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
      var hk = String.format("%s|%s", row.getPartitionKeys(), cl);
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
    });

    if (!sourceHashMap.isEmpty()) {
      QueryLedgerItemByPk queryLedgerItemByPk =
          new QueryLedgerItemByPk(
              row.getPartitionKeys(),
              Integer.parseInt(config.getProperty("TILE")),
              config.getProperty("TARGET_KEYSPACE"),
              config.getProperty("TARGET_TABLE"));

      List<Object> ledgerResultSet = ledgerStorageOnLevelDB.readRowMetaData(queryLedgerItemByPk);

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

    // Updating existing Items
    rowDiffering.forEach(
        (k, v) -> {
          var valueOnClient = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
          if (v.leftValue() > v.rightValue()) {
            LOGGER.debug(
                "Update a row in the target table {} by partition key: {} and clustering column: {}",
                config.getProperty("TARGET_TABLE"),
                row.getPartitionKeys(),
                k);
            var simpleStatement =
                SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                    .setIdempotent(true)
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

            var ledgerMetaData =
                new LedgerMetaData(
                    row.getPartitionKeys(),
                    k,
                    config.getProperty("TARGET_KEYSPACE"),
                    config.getProperty("TARGET_TABLE"),
                    Integer.parseInt(config.getProperty("TILE")),
                    valueOnClient,
                    v.leftValue());

            try {
              dataLoader(
                  simpleStatement,
                  ledgerMetaData,
                  Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
                  Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")),
                  "UPDATE");
            } catch (IOException e) {
              throw new RuntimeException();
            }
          }
        });

    // Adding new Items
    ConcurrentMap<String, Long> newConcurrentItems = new ConcurrentHashMap<>(newItems);

    newConcurrentItems.entrySet().parallelStream().forEach(
            (z) -> {
              var valueOnClient = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
              var simpleStatement =
                      SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(z.getKey()))
                              .setIdempotent(true)
                              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

              var ledgerMetaData =
                      new LedgerMetaData(
                              row.getPartitionKeys(),
                              z.getKey(),
                              config.getProperty("TARGET_KEYSPACE"),
                              config.getProperty("TARGET_TABLE"),
                              Integer.parseInt(config.getProperty("TILE")),
                              valueOnClient,
                              z.getValue());

              try {
                dataLoader(
                        simpleStatement,
                        ledgerMetaData,
                        Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
                        Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")),
                        "INSERT");
              } catch (IOException e) {
                try {
                  pkCache.remove(
                          Integer.parseInt(config.getProperty("TILE")),
                          "rd",
                          String.format(
                                  "%s|%s", row.getPartitionKeys(), z.getKey()));
                } catch (InterruptedException | ExecutionException | TimeoutException interruptedException) {
                  interruptedException.printStackTrace();
                }
                throw new RuntimeException();

              }
            });

    /*newItems.forEach(
        (k,v) -> {
          var valueOnClient = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
          var simpleStatement =
              SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                  .setIdempotent(true)
                  .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

          var ledgerMetaData =
              new LedgerMetaData(
                  row.getPartitionKeys(),
                  k,
                  config.getProperty("TARGET_KEYSPACE"),
                  config.getProperty("TARGET_TABLE"),
                  Integer.parseInt(config.getProperty("TILE")),
                  valueOnClient,
                  v);

          try {
            dataLoader(
                simpleStatement,
                ledgerMetaData,
                Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
                Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")),
                    "INSERT");
          } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException();
          }
        });
    */

    LOGGER.debug("Completed scanning rows in {}", row.getPartitionKeys());
  }

  @Override
  protected void doPerformTask(CacheStorage pkCache, Utils.CassandraTaskTypes taskName)
          throws InterruptedException, ExecutionException, TimeoutException, IOException {

    var partitionKeyNames =
        cassandraSchemaMetadata.get("partition_key").keySet().toArray(new String[0]);
    var clusteringColumnNames =
        cassandraSchemaMetadata.get("clustering").keySet().toArray(new String[0]);

    Stream<Object> ledgerPks = StreamSupport.stream(Spliterators.spliteratorUnknownSize(ledgerStorageOnLevelDB.readPaginatedPartitionsMetadata(), 0), true)
            .flatMap(List::stream);

    LOGGER.info(
        "The number of pre-loaded elements in the cache is {} ",
        pkCache.getSize(Integer.parseInt(config.getProperty("TILE"))));

    ledgerPks.parallel()
        .forEach(
            row -> {
              try {
                replicateCassandraRow((PrimaryKey) row, partitionKeyNames, clusteringColumnNames, pkCache);
              } catch (IOException e) {
                throw new RuntimeException();
              }
            });

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

    var statsMetaDataDeletes =
            new StatsMetaData(
                    Integer.parseInt(config.getProperty("TILE")),
                    config.getProperty("TARGET_KEYSPACE"),
                    config.getProperty("TARGET_TABLE"),
                    "DELETE");

    statsMetaDataDeletes.setValue(statsCounter.getStat("DELETE"));

    if (statsMetaDataDeletes.getValue() > 0) {
      targetStorageOnKeyspaces.writeStats(statsMetaDataDeletes);
    }

    statsCounter.resetStat("INSERT");
    statsCounter.resetStat("UPDATE");
    statsCounter.resetStat("DELETE");
  }
}
