// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.extractor;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.amazon.aws.cqlreplicator.models.PartitionMetaData;
import com.amazon.aws.cqlreplicator.models.PartitionsMetaData;
import com.amazon.aws.cqlreplicator.models.QueryLedgerItemByPk;
import com.amazon.aws.cqlreplicator.models.QueryStats;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.*;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

/** Responsible for extracting data from Amazon Keyspaces connection */
public class KeyspacesExtractor implements DataExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyspacesExtractor.class);
  private static CqlSession keyspacesSession;
  private static PreparedStatement psQueryLedgerItemByPk;
  private static PreparedStatement psPartitionsMetaData;
  private static PreparedStatement psPartitionMetaData;
  private static PreparedStatement psQueryStats;
  private static Retry retry;
  private static Retry.EventPublisher publisher;

  public KeyspacesExtractor(Properties config) {
    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(Integer.parseInt(config.getProperty("REPLICATE_RETRY_MAXATTEMPTS", "256")))
            .intervalFunction(IntervalFunction.ofExponentialBackoff(30, 1.5))
            .retryOnException(
                keyspacesExceptions -> keyspacesExceptions instanceof QueryConsistencyException)
            .retryExceptions(
                ReadFailureException.class,
                ReadTimeoutException.class,
                ServerError.class,
                UnavailableException.class)
            .failAfterMaxAttempts(true)
            .build();
    RetryRegistry registry = RetryRegistry.of(retryConfig);
    retry = registry.retry("KeyspacesReads");
    publisher = retry.getEventPublisher();

    ConnectionFactory connectionFactory = new ConnectionFactory(config);
    keyspacesSession = connectionFactory.buildCqlSession("KeyspacesConnector.conf");
    psQueryStats =
        keyspacesSession.prepare(
            "select tile, keyspacename, tablename, ops, rows from replicator.stats where ops='INSERT' and keyspacename=:keyspacename and tablename=:tablename allow filtering");
    psPartitionsMetaData =
        keyspacesSession.prepare(
            "SELECT pk, cc from replicator.ledger_v4 where process_name=:process_name and tile=:tile AND keyspacename=:keyspacename and tablename=:tablename AND pk=:pk");
    psQueryLedgerItemByPk =
        keyspacesSession.prepare(
            "select * from replicator.ledger_v4 where process_name=:process_name AND tile=:tile and keyspacename=:keyspacename and tablename=:tablename and pk=:pk");
    psPartitionMetaData =
        keyspacesSession.prepare(
            "select cc from replicator.ledger_v4 where process_name=:process_name and tile=:tile and keyspacename=:keyspacename and tablename=:tablename and pk=:pk limit 1");
  }

  @Override
  public List<Row> extract(final Object object) {
    BoundStatementBuilder boundStatementBuilder = null;

    if (object instanceof QueryLedgerItemByPk) {
      QueryLedgerItemByPk queryLedgerItemByPk = (QueryLedgerItemByPk) object;
      boundStatementBuilder =
          psQueryLedgerItemByPk
              .boundStatementBuilder()
              .setString("process_name", "rowDiscovery")
              .setInt("tile", queryLedgerItemByPk.getTile())
              .setString("keyspacename", queryLedgerItemByPk.getKeyspaceName())
              .setString("tablename", queryLedgerItemByPk.getTableName())
              .setString("pk", queryLedgerItemByPk.getPartitionKey());
    }

    if (object instanceof PartitionsMetaData) {
      PartitionsMetaData partitionsMetaData = (PartitionsMetaData) object;
      boundStatementBuilder =
          psPartitionsMetaData
              .boundStatementBuilder()
              .setString("process_name", "partitionDiscovery")
              .setInt("tile", partitionsMetaData.getTile())
              .setString("pk", String.valueOf(partitionsMetaData.getTile()))
              .setString("keyspacename", partitionsMetaData.getKeyspaceName())
              .setString("tablename", partitionsMetaData.getTableName());
    }

    if (object instanceof PartitionMetaData) {
      PartitionMetaData partitionMetaData = (PartitionMetaData) object;
      boundStatementBuilder =
          psPartitionMetaData
              .boundStatementBuilder()
              .setString("process_name", "partitionDiscovery")
              .setInt("tile", partitionMetaData.getTile())
              .setString("keyspacename", partitionMetaData.getKeyspaceName())
              .setString("tablename", partitionMetaData.getTableName())
              .setString("pk", partitionMetaData.getPk());
    }

    if (object instanceof QueryStats) {
      QueryStats queryStats = (QueryStats) object;
      boundStatementBuilder =
          psQueryStats
              .boundStatementBuilder()
              .setString("ops", queryStats.getOps())
              .setString("keyspacename", queryStats.getKeyspaceName())
              .setString("tablename", queryStats.getTableName());
    }

    BoundStatementBuilder finalBoundStatementBuilder = boundStatementBuilder;

    Supplier<List<Row>> querySupplier =
        () -> keyspacesSession.execute(finalBoundStatementBuilder.build()).all();

    List<Row> rs = Retry.decorateSupplier(retry, querySupplier).get();
    publisher.onError(event -> LOGGER.error(event.toString()));

    return rs;
  }
}
