// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator;

import com.amazon.aws.cqlreplicator.config.ConfigReader;
import com.amazon.aws.cqlreplicator.models.StatsAggrQuery;
import com.amazon.aws.cqlreplicator.storage.CacheStorage;
import com.amazon.aws.cqlreplicator.storage.MemcachedCacheStorage;
import com.amazon.aws.cqlreplicator.task.AbstractTask;
import com.amazon.aws.cqlreplicator.task.replication.CassandraReplicationTask;
import com.amazon.aws.cqlreplicator.task.replication.PartitionDiscoveryTask;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.amazon.aws.cqlreplicator.util.Utils.CassandraTaskTypes.SYNC_CASSANDRA_ROWS;
import static com.amazon.aws.cqlreplicator.util.Utils.CassandraTaskTypes.SYNC_DELETED_PARTITION_KEYS;

/** Responsible for initiating replication between Cassandra and Amazon Keyspaces */
@CommandLine.Command(
    name = "CQLReplicator",
    mixinStandardHelpOptions = true,
    version = "0.1",
    description = "Migration tool for Amazon Keyspaces")
public class Starter implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Starter.class);
  protected static Timer timer = new Timer("Timer");
  protected static TimerTask task;
  protected static Properties config;

  @CommandLine.Option(
      names = {"--pathToConfig"},
      description = "Path to config.properties file")
  private static String pathToConfig = "";

  private static long replicationDelay;
  private static long statsDelay;
  private static CacheStorage pkCacheForClusteringKeys;
  private static CacheStorage pkCacheForPartitionKeys;

  @CommandLine.Option(
      names = {"--syncPartitionKeys"},
      description = "Synchronizing partition keys by tile")
  private static boolean syncPartitionKeys = false;

  @CommandLine.Option(
      names = {"--syncClusteringColumns"},
      description = "Synchronizing clustering columns by tile")
  private static boolean syncClusteringColumns = false;

  @CommandLine.Option(
      names = {"--tile"},
      description = "Tile that should be processed by this instance")
  private static int tile;

  @CommandLine.Option(
      names = {"--tiles"},
      description = "The number of tiles")
  private static int tiles;

  @CommandLine.Option(
      names = {"--stats"},
      description = "Describe statistics")
  private static Boolean stats = false;

  private static AbstractTask abstractTaskClusteringKeys;
  private static AbstractTask abstractTaskPartitionKeys;
  private static Stats cqlReplicatorStats;
  CountDownLatch countDownLatch = new CountDownLatch(1);

  /** Responsible for running each task in a timer loop */
  public static void main(String[] args){
    var delay = 0L;
    var isStats = false;

    int arg = 0;
    for (String param : args) {
      if (param.equals("--pathToConfig")) {
        pathToConfig = args[arg + 1];
      }
      if (param.equals("--stats")) {
        isStats = true;
      }
      arg++;
    }

    if (pathToConfig.isBlank()) pathToConfig = System.getenv("CQLREPLICATOR_CONF");

    var configReader = new ConfigReader(pathToConfig);

    try {
      config = configReader.getConfig();
    } catch (IOException e) {
      LOGGER.error("Unable to read config.properties file due", e);
      System.exit(-1);
    }

    replicationDelay =
        TimeUnit.SECONDS.toMillis(
            Long.parseLong(config.getProperty("POOLING_PERIOD")));
    statsDelay =
        TimeUnit.SECONDS.toMillis(Long.parseLong(config.getProperty("POOLING_STATS_DATA")));

    if (isStats) delay = statsDelay;
    else delay = replicationDelay;

    if (config.getProperty("PRE_FLIGHT_CHECK").equals("true")) {
      config.setProperty("PATH_TO_CONFIG", pathToConfig);
      PreflightCheck preflightCheck = new PreflightCheck(config);
      try {
      preflightCheck.runPreFlightCheck();
      } catch (PreFlightCheckException e) {
        System.exit(-1);
      }
    }

    Runtime.getRuntime().addShutdownHook(new Thread(new Stopper()));

    task =
        new TimerTask() {
          @Override
          public void run() {
            new CommandLine(new Starter()).execute(args);
          }
        };
    timer.scheduleAtFixedRate(task, 0, delay);
  }

  /**
   * Creates CQLReplicator's tasks
   *
   * @return 0
   * @throws Exception
   */
  @Override
  public Integer call() throws Exception {

    /*
     * Set the current tile and tiles in config
     */

    config.setProperty("TILE", String.valueOf(tile));
    config.setProperty("TILES", String.valueOf(tiles));
    config.setProperty("PATH_TO_CONFIG", pathToConfig);


    if (syncPartitionKeys) {

      if (abstractTaskPartitionKeys == null) {
        config.setProperty("PROCESS_NAME", "pd");
        abstractTaskPartitionKeys = new PartitionDiscoveryTask(config);
        if (config.getProperty("EXTERNAL_MEMCACHED_STORAGE").equals("false")) {
        } else {
          pkCacheForPartitionKeys = new MemcachedCacheStorage(config, "pd");
        }
        pkCacheForPartitionKeys.connect();
      }

      LOGGER.info(
          "Partition keys synchronization process with refreshPeriodSec {} started at {}",
          replicationDelay,
          Instant.now());
      abstractTaskPartitionKeys.performTask(pkCacheForPartitionKeys, SYNC_DELETED_PARTITION_KEYS);
    }
    if (syncClusteringColumns) {
      if (abstractTaskClusteringKeys == null) {
        config.setProperty("PROCESS_NAME", "rd");
        abstractTaskClusteringKeys = new CassandraReplicationTask(config);
        if (config.getProperty("EXTERNAL_MEMCACHED_STORAGE").equals("false")) {
        } else {
          pkCacheForClusteringKeys = new MemcachedCacheStorage(config, "rd");
        }
        pkCacheForClusteringKeys.connect();
      }
      LOGGER.info(
          "Cassandra rows synchronization process with refreshPeriodSec {} started at {}",
          replicationDelay,
          Instant.now());
      abstractTaskClusteringKeys.performTask(pkCacheForClusteringKeys, SYNC_CASSANDRA_ROWS);
    }

    if (stats) {

      if (cqlReplicatorStats == null) {
        cqlReplicatorStats = new Stats(config);
      }

      Set<StatsAggrQuery> catalog = new HashSet<>();
      catalog.add(
          new StatsAggrQuery(
              "Total replicated operations by ops type to Amazon Keyspaces: {}",
              "SELECT (SELECT p.ops, SUM(p.\"rows\") AS totalByOps FROM resultSet AS p GROUP BY p.ops) AS result FROM inputDocument"));
      catalog.add(
          new StatsAggrQuery(
              "Total replicated operations by tiles to Amazon Keyspaces: {}",
              "SELECT (SELECT p.tile, SUM(p.\"rows\") AS totalByTile FROM resultSet AS p GROUP BY p.tile) AS result FROM inputDocument"));
      catalog.add(
          new StatsAggrQuery(
              "Total replicated operations to Amazon Keyspaces: {}",
              "SELECT (SELECT SUM(p.\"rows\") AS total FROM resultSet AS p) AS result FROM inputDocument"));

      Map<String, JsonNode> reports =
          cqlReplicatorStats.getStatsReport("SELECT json * FROM replicator.stats", catalog);
      for (Map.Entry<String, JsonNode> log : reports.entrySet()) {
        LOGGER.info(
            "------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        LOGGER.info(log.getKey(), log.getValue().get(0).get("result"));
      }
    }
    countDownLatch.countDown();
    return 0;
  }
}
