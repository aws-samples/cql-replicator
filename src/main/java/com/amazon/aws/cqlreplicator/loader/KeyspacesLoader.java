// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.loader;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.amazon.aws.cqlreplicator.models.*;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

/** Responsible for providing loading logic for target cluster */
public class KeyspacesLoader implements DataLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyspacesLoader.class);

  private static CqlSession keyspacesSession;

  private static Retry retry;
  private static Retry.EventPublisher publisher;
  private final Properties config;

  public KeyspacesLoader(Properties config) {
    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(
                Integer.parseInt(config.getProperty("REPLICATE_RETRY_MAXATTEMPTS","256")))
            .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofMillis(30), 1.5))
            .retryOnException(
                keyspacesExceptions ->
                   keyspacesExceptions
                        instanceof
                           QueryConsistencyException)
            .retryExceptions(
                WriteFailureException.class,
                WriteTimeoutException.class,
                ServerError.class,
                UnavailableException.class)
            .failAfterMaxAttempts(true)
            .build();
    RetryRegistry registry = RetryRegistry.of(retryConfig);
    retry = registry.retry("KeyspacesWrites");
    publisher = retry.getEventPublisher();

    this.config = config;
    ConnectionFactory connectionFactory = new ConnectionFactory(config);
    keyspacesSession = connectionFactory.getCassandraConnection("KeyspacesConnector.conf");
  }

  private BatchStatementBuilder prepareDeleteLegdgerStatement(
      DeleteTargetOperation deleteTargetOperation,
      QueryLedgerItemByPk deletePkLedger,
      PartitionMetaData partitionMetaData,
      BatchStatementBuilder batchableStatements) {
    PreparedStatement psDeletePkLedger =
        keyspacesSession.prepare("delete from replicator.ledger_v4 where process_name=:process_name and tile=:tile and keyspacename=:keyspacename and tablename=:tablename and pk=:pk");
    PreparedStatement psDeletePkPartitionKeys =
        keyspacesSession.prepare(
            "delete from replicator.ledger_v4 where process_name=:process_name and tile=:tile and keyspacename=:keyspacename and tablename=:tablename and pk=:pk and cc=:cc");
    PreparedStatement psInsertDeletedPartitions =
        keyspacesSession.prepare(
            "insert into replicator.deleted_partitions(tile, pk, keyspacename, tablename, deleted_ts) values (:tile, :pk, :keyspacename, :tablename, :deleted_ts)");
    BoundStatementBuilder bsDeletePkLedger;
    BoundStatementBuilder bsDeletePkPartitionKeys;
    BoundStatementBuilder bsInsertDeletedPartitions;

    // Let's check if a delete operation occurs
    if (deleteTargetOperation != null
        && deletePkLedger != null
        && partitionMetaData != null
        ) {
      // Deleting the row in the target table
      List<String> whereClause = new ArrayList<>();
      for (String col : deleteTargetOperation.getNames()) {
        whereClause.add(String.format("%s=:%s", col, col));
      }
      String finalWhereClause = String.join(" AND ", whereClause);

      // Build the DELETE statement
      String deleteStatement =
          String.format(
              "DELETE FROM %s.%s WHERE %s",
              deleteTargetOperation.getKeyspaceName(),
              deleteTargetOperation.getTableName(),
              finalWhereClause);

      PreparedStatement psDeleteTargetData = keyspacesSession.prepare(deleteStatement);
      BoundStatementBuilder bsDeleteTargetData = psDeleteTargetData.boundStatementBuilder();

      int i = 0;
      for (String cl : deleteTargetOperation.getNames()) {
        String type = deleteTargetOperation.getTypes().get(cl);
          bsDeleteTargetData =
              Utils.aggregateBuilder(
                  type, cl, deleteTargetOperation.getValues()[i], bsDeleteTargetData);
        i++;
      }

      // Deleting the row in the ledger table
      bsDeletePkLedger =
          psDeletePkLedger
              .boundStatementBuilder()
              .setString("process_name", "rowDiscovery")
              .setInt("tile", deletePkLedger.getTile())
              .setString("keyspacename", deletePkLedger.getKeyspaceName())
              .setString("tablename", deletePkLedger.getTableName())
              .setString("pk", deletePkLedger.getPartitionKey());

      // Deleting the row in the partitionkeys table
      bsDeletePkPartitionKeys =
          psDeletePkPartitionKeys
              .boundStatementBuilder()
              .setString("process_name", "partitionDiscovery")
              .setInt("tile", partitionMetaData.getTile())
              .setString("keyspacename", partitionMetaData.getKeyspaceName())
              .setString("tablename", partitionMetaData.getTableName())
              .setString("pk", String.valueOf(partitionMetaData.getTile()))
              .setString("cc", partitionMetaData.getPk());

      if (config.getProperty("REGISTER_PARTITION_DELETES").equals("true")) {
        long ts = Instant.now().toEpochMilli();
        bsInsertDeletedPartitions =
            psInsertDeletedPartitions
                .boundStatementBuilder()
                .setInt("tile", partitionMetaData.getTile())
                .setString("pk", partitionMetaData.getPk())
                .setString("keyspacename", partitionMetaData.getKeyspaceName())
                .setString("tablename", partitionMetaData.getTableName())
                .setLong("deleted_ts", ts);

        batchableStatements.addStatements(
            bsInsertDeletedPartitions
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setIdempotence(true)
                .build());
      }

      batchableStatements.addStatements(
          bsDeleteTargetData
              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .setIdempotence(true)
              .build(),
          bsDeletePkLedger
              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .setIdempotence(true)
              .build(),
          bsDeletePkPartitionKeys
              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .setIdempotence(true)
              .build());
    }
    return batchableStatements;
  }

  @Override
  public void load(Object... objects) {
    PreparedStatement psRetryMetaData =
        keyspacesSession.prepare(
            "update replicator.retries set cnt=cnt+1 where "
                + "dt_retries=:dt_retries and pk=:pk and cl=:cl and ops=:ops and keyspacename=:keyspacename and tablename=:tablename");
    PreparedStatement psStatsMetaData =
        keyspacesSession.prepare(
            "update replicator.stats set rows=rows+:value where tile=:tile and keyspacename=:keyspacename and tablename=:tablename and ops=:ops");
    PreparedStatement psLedgerMetaData =
        keyspacesSession.prepare(
            "insert into replicator.ledger_v4(process_name, tile, keyspacename,tablename, pk, cc, operation_ts, value) values(:process_name, :tile, :keyspacename, :tablename, :pk, :cc, :operation_ts, :value)");
    PreparedStatement psUpdatePartitionKeys =
        keyspacesSession.prepare(
            "insert into replicator.ledger_v4(process_name, tile, keyspacename,tablename, pk, cc, operation_ts, value) values(:process_name, :tile, :keyspacename, :tablename, :pk, :cc, :operation_ts, :value)");

    Statement<?> statement = null;
    LedgerMetaData ledgerMetaData = null;
    StatsMetaData statsMetaData = null;
    RetryEntry retryEntry = null;
    PartitionMetaData partitionMetaData = null;
    QueryLedgerItemByPk deletePkLedger = null;
    DeleteTargetOperation deleteTargetOperation = null;

    for (Object obj : objects) {
      if (obj instanceof Statement<?>) statement = (Statement<?>) obj;
      if (obj instanceof LedgerMetaData) ledgerMetaData = (LedgerMetaData) obj;
      if (obj instanceof StatsMetaData) statsMetaData = (StatsMetaData) obj;
      if (obj instanceof RetryEntry) retryEntry = (RetryEntry) obj;
      if (obj instanceof PartitionMetaData) partitionMetaData = (PartitionMetaData) obj;
      if (obj instanceof DeleteTargetOperation) deleteTargetOperation = (DeleteTargetOperation) obj;
      if (obj instanceof QueryLedgerItemByPk) deletePkLedger = (QueryLedgerItemByPk) obj;
    }

    BoundStatementBuilder bsUpdatePartitionKeys;
    BoundStatementBuilder bsStatsMetaData;
    BoundStatementBuilder bsLedgerMetaData;
    BoundStatementBuilder bsInsertPartitionKeys;

    BatchStatementBuilder batchableStatements = BatchStatement.builder(DefaultBatchType.UNLOGGED);

    /*
    Delete data from the Target table
    Delete data from the Ledger table
    Delete data from the Partitionkeys table
    */

    if (config.getProperty("TRANSFORM_INBOUND_REQUEST").equals("false")) {
      batchableStatements =
          prepareDeleteLegdgerStatement(
              deleteTargetOperation, deletePkLedger, partitionMetaData, batchableStatements);
    }

    // Execute batch logic
    if (statement == null && partitionMetaData != null && deleteTargetOperation == null) {

      ZonedDateTime valueOnClient = ZonedDateTime.now();

      bsInsertPartitionKeys =
          psUpdatePartitionKeys
              .boundStatementBuilder()
              .setString("process_name", "partitionDiscovery")
              .setInt("tile", partitionMetaData.getTile())
              .setString("keyspacename", partitionMetaData.getKeyspaceName())
              .setString("tablename", partitionMetaData.getTableName())
              .setString("pk", String.valueOf(partitionMetaData.getTile()))
              .setString("cc", partitionMetaData.getPk())
              .setLong("value", 1)
              .set("operation_ts", valueOnClient, GenericType.ZONED_DATE_TIME)
              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

      batchableStatements.addStatement(bsInsertPartitionKeys.build());
    }

    if (statement == null
        && statsMetaData != null
        && deleteTargetOperation == null
        && partitionMetaData == null) {
      bsStatsMetaData =
          psStatsMetaData
              .boundStatementBuilder()
              .setInt("tile", statsMetaData.getTile())
              .setString("keyspacename", statsMetaData.getKeyspaceName())
              .setString("tablename", statsMetaData.getTableName())
              .setString("ops", statsMetaData.getOps())
              .setLong("value", statsMetaData.getValue())
              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

      batchableStatements.addStatements(bsStatsMetaData.build());
    }

    if (statement != null) batchableStatements.addStatement((BatchableStatement<?>) statement);

    if (ledgerMetaData != null && statsMetaData != null) {

      ZonedDateTime valueOnClient = ZonedDateTime.now();

      bsUpdatePartitionKeys =
          psUpdatePartitionKeys
              .boundStatementBuilder()
              .setString("process_name", "partitionDiscovery")
              .setInt("tile", statsMetaData.getTile())
              .setString("keyspacename", statsMetaData.getKeyspaceName())
              .setString("tablename", statsMetaData.getTableName())
              .setString("pk", String.valueOf(ledgerMetaData.getTile()))
              .setString("cc", ledgerMetaData.getPartitionKeys())
              .setLong("value", 1)
              .set("operation_ts", valueOnClient, GenericType.ZONED_DATE_TIME)
              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

      bsLedgerMetaData =
          psLedgerMetaData
              .boundStatementBuilder()
              .setString("process_name", "rowDiscovery")
              .setInt("tile", ledgerMetaData.getTile())
              .setString("pk", ledgerMetaData.getPartitionKeys())
              .setString("cc", ledgerMetaData.getClusteringColumns())
              .setString("keyspacename", ledgerMetaData.getKeyspaceName())
              .setString("tablename", ledgerMetaData.getTableName())
              .set("operation_ts", ledgerMetaData.getLastRun(), GenericType.ZONED_DATE_TIME)
              .setLong("value", ledgerMetaData.getLastWriteTime())
              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

      if (statsMetaData.getOps().equals("INSERT")) {
        batchableStatements.addStatement(bsUpdatePartitionKeys.build());
      }
      batchableStatements.addStatements(bsLedgerMetaData.build());
    }
    BatchStatementBuilder finalBatchableStatements = batchableStatements;

    Supplier<List<Row>> batchSupplier =
        () -> keyspacesSession.execute(finalBatchableStatements.build()).all();
    Retry.decorateSupplier(retry, batchSupplier).get();
    RetryEntry finalRetryEntry = retryEntry;

    publisher.onError(
            event -> {
              keyspacesSession.execute(
                      psRetryMetaData
                              .boundStatementBuilder()
                              .setLocalDate("dt_retries", finalRetryEntry.getDt_retries())
                              .setString("pk", finalRetryEntry.getPartitionKey())
                              .setString("cl", finalRetryEntry.getClusteringColumns())
                              .setString("ops", finalRetryEntry.getOps())
                              .setString("keyspacename", finalRetryEntry.getKeyspaceName())
                              .setString("tablename", finalRetryEntry.getTableName())
                              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                              .build());
              LOGGER.warn("Operation was failed on event {}", event.toString());
            });
  }
}
