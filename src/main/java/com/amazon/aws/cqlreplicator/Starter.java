// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator;

import com.amazon.aws.cqlreplicator.config.ConfigReader;
import com.amazon.aws.cqlreplicator.storage.StorageServiceImpl;
import com.amazon.aws.cqlreplicator.task.AbstractTaskV2;
import com.amazon.aws.cqlreplicator.task.replication.CassandraReplicationTaskV2;
import com.amazon.aws.cqlreplicator.task.replication.PartitionDiscoveryTaskV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

import static com.amazon.aws.cqlreplicator.util.Utils.CassandraTaskTypes.SYNC_CASSANDRA_ROWS;
import static com.amazon.aws.cqlreplicator.util.Utils.CassandraTaskTypes.SYNC_DELETED_PARTITION_KEYS;

/**
 * Responsible for initiating replication between Cassandra and Amazon Keyspaces
 */
@CommandLine.Command(
        name = "CQLReplicator",
        mixinStandardHelpOptions = true,
        version = "1.1",
        description = "Migration tool for Amazon Keyspaces")
public class Starter implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Starter.class);
    private static final int numCores = Runtime.getRuntime().availableProcessors();
    private static final long allocatedMemory = Runtime.getRuntime().totalMemory();
    protected static Timer timer = new Timer("Timer");
    protected static TimerTask task;
    protected static Properties config;
    @CommandLine.Option(
            names = {"--pathToConfig"},
            description = "Path to config.properties file")
    private static String pathToConfig = "";

    private static long replicationDelay;
    private static long statsDelay;

    @CommandLine.Option(
            names = {"--tile"},
            description = "Tile that should be processed by this instance")
    private static int tile;

    @CommandLine.Option(
            names = {"--tiles"},
            description = "The number of tiles")
    private static int tiles;

    private static AbstractTaskV2 abstractTaskClusteringKeys;
    private static AbstractTaskV2 abstractTaskPartitionKeys;
    private static StorageServiceImpl storageService;
    CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * Responsible for running each task in a timer loop
     */
    public static void main(String[] args) throws IOException {

        var delay = 0L;

        int arg = 0;
        for (String param : args) {
            if (param.equals("--pathToConfig")) {
                pathToConfig = args[arg + 1];
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

        LOGGER.info(String.format("Available CPUs: %s", numCores));
        LOGGER.info(String.format("Allocated memory: %s", allocatedMemory));

        replicationDelay =
                TimeUnit.SECONDS.toMillis(
                        Long.parseLong(config.getProperty("POOLING_PERIOD")));
        statsDelay =
                TimeUnit.SECONDS.toMillis(Long.parseLong(config.getProperty("POOLING_STATS_DATA")));

        delay = replicationDelay;

        if (config.getProperty("PRE_FLIGHT_CHECK").equals("true")) {
            config.setProperty("PATH_TO_CONFIG", pathToConfig);
            PreflightCheck preflightCheck = new PreflightCheck(config);
            try {
                preflightCheck.runPreFlightCheck();
            } catch (PreFlightCheckException e) {
                System.exit(-1);
            }
        }

        config.setProperty("TILE", String.valueOf(args[1]));
        storageService = new StorageServiceImpl(config);

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

        final int numThreads = Math.max(1, ((numCores >> 1) - 1));

        /*
         * Set the current tile and tiles in config
         */

        config.setProperty("TILE", String.valueOf(tile));
        config.setProperty("TILES", String.valueOf(tiles));
        config.setProperty("PATH_TO_CONFIG", pathToConfig);


        if (abstractTaskPartitionKeys == null) {
            config.setProperty("PROCESS_NAME", "pd");
            abstractTaskPartitionKeys = new PartitionDiscoveryTaskV2(config);
        }

        LOGGER.debug(
                "Partition keys synchronization process with refreshPeriodSec {} started at {}",
                replicationDelay,
                Instant.now());

        var pdExec = Executors.newFixedThreadPool(numThreads);
        pdExec.execute(() -> {
            try {
                abstractTaskPartitionKeys.performTask(storageService, SYNC_DELETED_PARTITION_KEYS);
            } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });

        if (abstractTaskClusteringKeys == null) {
            config.setProperty("PROCESS_NAME", "rd");
            abstractTaskClusteringKeys = new CassandraReplicationTaskV2(config);
        }
        LOGGER.debug(
                "Cassandra rows synchronization process with refreshPeriodSec {} started at {}",
                replicationDelay,
                Instant.now());

        abstractTaskClusteringKeys.performTask(storageService, SYNC_CASSANDRA_ROWS);
        countDownLatch.countDown();
        return 0;
    }
}
