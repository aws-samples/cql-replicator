/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.amazon.aws.cqlreplicator.models.LedgerMetaData;
import com.amazon.aws.cqlreplicator.models.PartitionMetaData;
import com.amazon.aws.cqlreplicator.models.PartitionsMetaData;
import com.amazon.aws.cqlreplicator.models.QueryLedgerItemByPk;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import io.github.resilience4j.core.IntervalFunction;
//import io.github.resilience4j.ratelimiter.RateLimiter;
//import io.github.resilience4j.ratelimiter.RateLimiterConfig;
//import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class LedgerStorageOnKeyspaces
    extends LedgerStorage<Object, List<Row>, BoundStatementBuilder> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LedgerStorageOnKeyspaces.class);
  private static Retry retry;
  //private static RateLimiter limiter;
  private static Retry.EventPublisher publisher;
  //private static RateLimiter.EventPublisher publisher1;

  private static CqlSession cqlSession;
  private static PreparedStatement psWriteRowMetadata;
  private static PreparedStatement psWritePartitionMetadata;
  private static PreparedStatement psDeletePartitionMetadata;
  private static PreparedStatement psDeleteRowMetadata;
  private static PreparedStatement psReadPartitionsMetadata;
  private static PreparedStatement psReadRowMetaData;
  private static PreparedStatement psReadPartitionMetadata;

  public LedgerStorageOnKeyspaces(Properties properties) {
    var connectionFactory = new ConnectionFactory(properties);
    cqlSession = connectionFactory.buildCqlSession("KeyspacesConnector.conf");
    psWriteRowMetadata =
        cqlSession.prepare(
            "insert into replicator.ledger_v4(process_name, tile, keyspacename,tablename, pk, cc, operation_ts, value) values(:process_name, :tile, :keyspacename, :tablename, :pk, :cc, :operation_ts, :value)");
    psWritePartitionMetadata =
        cqlSession.prepare(
            "insert into replicator.ledger_v4(process_name, tile, keyspacename,tablename, pk, cc, operation_ts, value) values(:process_name, :tile, :keyspacename, :tablename, :pk, :cc, :operation_ts, :value)");
    psDeleteRowMetadata =
        cqlSession.prepare(
            "delete from replicator.ledger_v4 where process_name=:process_name and tile=:tile and keyspacename=:keyspacename and tablename=:tablename and pk=:pk");
    psDeletePartitionMetadata =
        cqlSession.prepare(
            "delete from replicator.ledger_v4 where process_name=:process_name and tile=:tile and keyspacename=:keyspacename and tablename=:tablename and pk=:pk and cc=:cc");
    psReadPartitionsMetadata =
        cqlSession.prepare(
            "SELECT pk, cc from replicator.ledger_v4 where process_name=:process_name and tile=:tile AND keyspacename=:keyspacename and tablename=:tablename AND pk=:pk");
    psReadRowMetaData =
        cqlSession.prepare(
            "select * from replicator.ledger_v4 where process_name=:process_name AND tile=:tile and keyspacename=:keyspacename and tablename=:tablename and pk=:pk");
    psReadPartitionMetadata =
        cqlSession.prepare(
            "select cc from replicator.ledger_v4 where process_name=:process_name and tile=:tile and keyspacename=:keyspacename and tablename=:tablename and pk=:pk limit 1");

    var retryConfig =
        RetryConfig.custom()
            .maxAttempts(
                Integer.parseInt(properties.getProperty("REPLICATE_RETRY_MAXATTEMPTS", "256")))
            .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofMillis(30), 1.5))
            .retryOnException(
                keyspacesExceptions -> keyspacesExceptions instanceof QueryConsistencyException)
            .retryExceptions(
                WriteFailureException.class,
                WriteTimeoutException.class,
                ServerError.class,
                UnavailableException.class,
                ReadFailureException.class,
                ReadTimeoutException.class,
                CoordinatorException.class,
                FunctionFailureException.class)
            .failAfterMaxAttempts(true)
            .build();
    var registry = RetryRegistry.of(retryConfig);
    retry = registry.retry("LedgerStorageKeyspaces");
    publisher = retry.getEventPublisher();

    /*
    var limiterConfig =
        RateLimiterConfig.custom()
            .limitForPeriod(Integer.parseInt(properties.getProperty("RATELIMITER_PERMITS")))
            .limitRefreshPeriod(Duration.ofMillis(500))
            .timeoutDuration(Duration.ofMillis(Integer.parseInt(properties.getProperty("RATELIMITER_TIMEOUT_MS"))))
            .build();
    var registryRL = RateLimiterRegistry.of(limiterConfig);
    limiter = registryRL.rateLimiter("LedgerStorageKeyspaces");
    publisher1 = limiter.getEventPublisher();

     */
  }

  @Override
  public void tearDown() {
    cqlSession.close();
  }

  @Override
  public void writePartitionMetadata(Object o) {
    var valueOnClient = ZonedDateTime.now();
    var partitionMetadata = (PartitionMetaData) o;
    var bsPartitionMetadata =
        psWritePartitionMetadata
            .boundStatementBuilder()
            .setString("process_name", "partitionDiscovery")
            .setInt("tile", partitionMetadata.getTile())
            .setString("keyspacename", partitionMetadata.getKeyspaceName())
            .setString("tablename", partitionMetadata.getTableName())
            .setString("pk", String.valueOf(partitionMetadata.getTile()))
            .setString("cc", partitionMetadata.getPk())
            .setLong("value", 1)
            .set("operation_ts", valueOnClient, GenericType.ZONED_DATE_TIME)
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .setIdempotence(true);
    execute(bsPartitionMetadata);
  }

  @Override
  public List<Row> readPartitionMetadata(Object o) {
    var partitionMetadata = (PartitionMetaData) o;
    var bsReadPartitionMetadata =
        psReadPartitionMetadata
            .boundStatementBuilder()
            .setString("process_name", "partitionDiscovery")
            .setInt("tile", partitionMetadata.getTile())
            .setString("keyspacename", partitionMetadata.getKeyspaceName())
            .setString("tablename", partitionMetadata.getTableName())
            .setString("pk", partitionMetadata.getPk())
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    return execute(bsReadPartitionMetadata);
  }

  @Override
  public void writeRowMetadata(Object o) {
    var rowMetadata = (LedgerMetaData) o;
    var bsRowMetadata =
        psWriteRowMetadata
            .boundStatementBuilder()
            .setString("process_name", "rowDiscovery")
            .setInt("tile", rowMetadata.getTile())
            .setString("pk", rowMetadata.getPartitionKeys())
            .setString("cc", rowMetadata.getClusteringColumns())
            .setString("keyspacename", rowMetadata.getKeyspaceName())
            .setString("tablename", rowMetadata.getTableName())
            .set("operation_ts", rowMetadata.getLastRun(), GenericType.ZONED_DATE_TIME)
            .setLong("value", rowMetadata.getLastWriteTime())
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .setIdempotence(true);
    execute(bsRowMetadata);
  }

  @Override
  public List<Row> readRowMetaData(Object o) {
    var queryRowMetadata = (QueryLedgerItemByPk) o;
    var bsReadRowMetadata =
        psReadRowMetaData
            .boundStatementBuilder()
            .setString("process_name", "rowDiscovery")
            .setInt("tile", queryRowMetadata.getTile())
            .setString("keyspacename", queryRowMetadata.getKeyspaceName())
            .setString("tablename", queryRowMetadata.getTableName())
            .setString("pk", queryRowMetadata.getPartitionKey())
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    return execute(bsReadRowMetadata);
  }

  @Override
  public void deletePartitionMetadata(Object o) {
    var partitionMetadata = (PartitionMetaData) o;
    var bsDeletePkPartitionMetadata =
        psDeletePartitionMetadata
            .boundStatementBuilder()
            .setString("process_name", "partitionDiscovery")
            .setInt("tile", partitionMetadata.getTile())
            .setString("keyspacename", partitionMetadata.getKeyspaceName())
            .setString("tablename", partitionMetadata.getTableName())
            .setString("pk", String.valueOf(partitionMetadata.getTile()))
            .setString("cc", partitionMetadata.getPk())
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .setIdempotence(true);
    execute(bsDeletePkPartitionMetadata);
  }

  @Override
  public void deleteRowMetadata(Object o) {
    var rowMetadata = (QueryLedgerItemByPk) o;
    var bsDeleteRowMetadata =
        psDeleteRowMetadata
            .boundStatementBuilder()
            .setString("process_name", "rowDiscovery")
            .setInt("tile", rowMetadata.getTile())
            .setString("keyspacename", rowMetadata.getKeyspaceName())
            .setString("tablename", rowMetadata.getTableName())
            .setString("pk", rowMetadata.getPartitionKey());
    execute(bsDeleteRowMetadata);
  }

  @Override
  public List<Row> readPartitionsMetadata(Object o) {
    var partitionsMetadata = (PartitionsMetaData) o;
    var boundStatementBuilder =
        psReadPartitionsMetadata
            .boundStatementBuilder()
            .setString("process_name", "partitionDiscovery")
            .setInt("tile", partitionsMetadata.getTile())
            .setString("pk", String.valueOf(partitionsMetadata.getTile()))
            .setString("keyspacename", partitionsMetadata.getKeyspaceName())
            .setString("tablename", partitionsMetadata.getTableName())
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    return execute(boundStatementBuilder);
  }

  @Override
  public List<Row> execute(BoundStatementBuilder boundStatementBuilder) {
    Supplier<List<Row>> supplier = () -> cqlSession.execute(boundStatementBuilder.build()).all();
    publisher.onError(event -> LOGGER.error("Operation was failed on event {}", event.toString()));
    publisher.onRetry(event -> LOGGER.warn("Operation was retried on event {}", event.toString()));
    return Retry.decorateSupplier(retry, supplier).get();

    /*
    Supplier<List<Row>> rateLimiterSupplier =
        RateLimiter.decorateSupplier(
            limiter, () -> cqlSession.execute(boundStatementBuilder.build()).all());
    publisher1.onSuccess(event -> LOGGER.debug("{}", event.toString()));
    Supplier<List<Row>> supplier = Retry.decorateSupplier(retry, rateLimiterSupplier);
    publisher.onRetry(event -> LOGGER.warn("Operation was retried on event {}", event.toString()));
    publisher.onError(event -> LOGGER.error("Operation was failed on event {}", event.toString()));
    return Retry.decorateSupplier(retry, supplier).get();
     */
  }
}
