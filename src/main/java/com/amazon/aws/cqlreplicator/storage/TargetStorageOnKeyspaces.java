/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.amazon.aws.cqlreplicator.models.DeleteTargetOperation;
import com.amazon.aws.cqlreplicator.models.QueryStats;
import com.amazon.aws.cqlreplicator.models.StatsMetaData;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.servererrors.*;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class TargetStorageOnKeyspaces
    extends TargetStorage<Object, List<Row>, BatchStatementBuilder, SimpleStatement> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TargetStorageOnKeyspaces.class);
  private static CqlSession cqlSession;
  private static PreparedStatement psWriteStats;
  private static PreparedStatement psReadStats;
  //private static RateLimiter limiter;
  //private static RateLimiter.EventPublisher publisher1;
  private Retry retry;
  private Retry.EventPublisher publisher;

  public TargetStorageOnKeyspaces(Properties properties) {
    var connectionFactory = new ConnectionFactory(properties);
    cqlSession = connectionFactory.buildCqlSession("KeyspacesConnector.conf");

    psWriteStats =
        cqlSession.prepare(
            "update replicator.stats set rows=rows+:value where tile=:tile and keyspacename=:keyspacename and tablename=:tablename and ops=:ops");
    psReadStats =
        cqlSession.prepare(
            "select tile, keyspacename, tablename, ops, rows from replicator.stats where ops=:ops and keyspacename=:keyspacename and tablename=:tablename allow filtering");

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
    retry = registry.retry("TargetKeyspacesStorage");
    publisher = retry.getEventPublisher();

    /*
    var limiterConfig =
        RateLimiterConfig.custom()
                .limitForPeriod(Integer.parseInt(properties.getProperty("RATELIMITER_PERMITS")))
                .limitRefreshPeriod(Duration.ofMillis(500))
                .timeoutDuration(Duration.ofMillis(Integer.parseInt(properties.getProperty("RATELIMITER_TIMEOUT_MS"))))
                .build();
    var registryRL = RateLimiterRegistry.of(limiterConfig);
    limiter = registryRL.rateLimiter("TargetKeyspacesStorage");
    publisher1 = limiter.getEventPublisher();

     */
  }

  @Override
  public void tearDown() {
    cqlSession.close();
  }

  @Override
  public List<Row> execute(BatchStatementBuilder batchableStatement) {
    Supplier<List<Row>> supplier = () -> cqlSession.execute(batchableStatement.build()).all();
    publisher.onError(event -> LOGGER.error("Operation was failed on event {}", event.toString()));
    publisher.onRetry(event -> LOGGER.warn("Operation was retried on event {}", event.toString()));
    return Retry.decorateSupplier(retry, supplier).get();

    /*
    Supplier<List<Row>> rateLimiterSupplier =
        RateLimiter.decorateSupplier(
            limiter, () -> cqlSession.execute(batchableStatement.build()).all());
    publisher1.onSuccess(event -> LOGGER.debug("{}", event.toString()));
    Supplier<List<Row>> supplier = Retry.decorateSupplier(retry, rateLimiterSupplier);
    publisher.onRetry(event -> LOGGER.warn("Operation was retried on event {}", event.toString()));
    publisher.onError(event -> LOGGER.error("Operation was failed on event {}", event.toString()));
    return Retry.decorateSupplier(retry, supplier).get();
     */
  }

  @Override
  public void write(SimpleStatement statement) {
    var batchableStatements = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    batchableStatements.addStatement(statement);
    execute(batchableStatements);
  }

  @Override
  public void writeStats(Object o) {
    var statsMetadata = (StatsMetaData) o;
    var boundStatementBuilder =
        psWriteStats
            .boundStatementBuilder()
            .setInt("tile", statsMetadata.getTile())
            .setString("keyspacename", statsMetadata.getKeyspaceName())
            .setString("tablename", statsMetadata.getTableName())
            .setString("ops", statsMetadata.getOps())
            .setLong("value", statsMetadata.getValue())
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

    var batchableStatements = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    batchableStatements.addStatement(
        boundStatementBuilder.setConsistencyLevel(ConsistencyLevel.QUORUM).build());

    execute(batchableStatements);
  }

  @Override
  public List<Row> readStats(Object o) {
    var queryStats = (QueryStats) o;
    var boundStatementBuilder =
        psReadStats
            .boundStatementBuilder()
            .setString("ops", queryStats.getOps())
            .setString("keyspacename", queryStats.getKeyspaceName())
            .setString("tablename", queryStats.getTableName());
    var batchableStatements = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    batchableStatements.addStatement(
        boundStatementBuilder.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE).build());

    return execute(batchableStatements);
  }

  @Override
  public void delete(Object o) {
    var batchableStatements = BatchStatement.builder(DefaultBatchType.UNLOGGED);

    var deleteTargetOperation = (DeleteTargetOperation) o;
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

    PreparedStatement psDeleteTargetData = cqlSession.prepare(deleteStatement);
    BoundStatementBuilder bsDeleteTargetData = psDeleteTargetData.boundStatementBuilder();

    int i = 0;
    for (String cl : deleteTargetOperation.getNames()) {
      String type = deleteTargetOperation.getTypes().get(cl);
      bsDeleteTargetData =
          Utils.aggregateBuilder(
              type, cl, deleteTargetOperation.getValues()[i], bsDeleteTargetData);
      i++;
    }

    batchableStatements.addStatement(
        bsDeleteTargetData
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .setIdempotence(true)
            .build());

    execute(batchableStatements);
  }
}
