// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator;

import com.amazon.aws.cqlreplicator.config.ConfigReader;
import com.amazon.aws.cqlreplicator.storage.StorageServiceImpl;
import com.amazon.aws.cqlreplicator.task.AbstractTaskV2;
import com.amazon.aws.cqlreplicator.task.replication.DeletedRowDiscoveryTask;
import com.amazon.aws.cqlreplicator.task.replication.PartitionDiscoveryTaskV2;
import com.amazon.aws.cqlreplicator.task.replication.RowDiscoveryTaskV2;
import com.amazon.aws.cqlreplicator.util.ApiEndpoints;
import com.datastax.oss.driver.api.core.DriverException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

import static com.amazon.aws.cqlreplicator.util.Utils.CassandraTaskTypes.*;

/**
 * Responsible for initiating replication between Cassandra and Amazon Keyspaces
 */
@CommandLine.Command(
        name = "CQLReplicator",
        mixinStandardHelpOptions = true,
        version = "1.1",
        description = "Migration tool for Amazon Keyspaces")
public class Starter implements Callable<Integer> {

    final static int HTTP_PORT = 8080;
    final static int BLOCKING_QUEUE_SIZE = 15000;
    private static void httpHealthCheck() {

        Vertx vertx = Vertx.vertx();

        var healthCheckHandler = HealthCheckHandler.create(vertx);

        var router = Router.router(vertx);

        healthCheckHandler.register("cql-replicator-health", promise -> {
            // Upon success do
            promise.complete(Status.OK());
            // In case of failure do:
            promise.complete(Status.KO());
        });

        router.get(ApiEndpoints.HEALTH_ROUTE).handler(healthCheckHandler);

        vertx.createHttpServer()
                // Handle every request using the router
                .requestHandler(router)
                // Start listening
                .listen(HTTP_PORT, http -> {
                    if (http.succeeded()) {
                        LOGGER.info("HTTP server started on port {}", HTTP_PORT);
                    }
                });
    }

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
    private static AbstractTaskV2 abstractTaskDeletedRows;
    private static StorageServiceImpl storageService;
    protected static CountDownLatch countDownLatch;
    protected static BlockingQueue<Runnable> blockingQueue;
    protected static ThreadPoolExecutor rowExecutor;
    protected static ExecutorService pdExecutor;
    protected static ExecutorService dpdExecutor;
    private static MeterRegistry meterRegistry;

    /**
     * Responsible for running each task in a timer loop
     */
    public static void main(String[] args) throws Exception {

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
            try (var preflightCheck = new PreflightCheck(config)) {
                preflightCheck.runPreFlightCheck();
            } catch (PreFlightCheckException | DriverException e) {
                LOGGER.error(e.getMessage());
                System.exit(-1);
            }
        }

        Starter.blockingQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_SIZE);
        Starter.rowExecutor = new ThreadPoolExecutor(
                Integer.parseInt(config.getProperty("REPLICATE_WITH_CORE_POOL_SIZE")),
                Integer.parseInt(config.getProperty("REPLICATE_WITH_MAX_CORE_POOL_SIZE")),
                Integer.parseInt(config.getProperty("REPLICATE_WITH_CORE_POOL_TIMEOUT")),
                TimeUnit.SECONDS,
                blockingQueue,
                new ThreadPoolExecutor.AbortPolicy());

        Starter.pdExecutor = Executors.newFixedThreadPool(numCores - 1);
        Starter.dpdExecutor = Executors.newFixedThreadPool(numCores - 1);

        config.setProperty("TILE", String.valueOf(args[1]));
        storageService = new StorageServiceImpl(config);

        if (config.getProperty("ENABLE_CLOUD_WATCH").equals("true")) {
            CloudWatchCustomMetrics customMetrics = new CloudWatchCustomMetrics(
                    CloudWatchAsyncClient.builder().region(Region.of(config.getProperty("CLOUD_WATCH_REGION"))).build());
            meterRegistry = customMetrics.getMeterRegistry();
        } else
        {
            meterRegistry = new SimpleMeterRegistry();
        }

        // TODO: Set two options 2 - without deletes and 3 - with deletes
        if (config.getProperty("REPLICATE_DELETES").equals("true")) {
            countDownLatch = new CountDownLatch(3);
        } else {
            countDownLatch = new CountDownLatch(2);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Stopper()));
        httpHealthCheck();

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
        var startTime = System.nanoTime();

        config.setProperty("TILE", String.valueOf(tile));
        config.setProperty("TILES", String.valueOf(tiles));
        config.setProperty("PATH_TO_CONFIG", pathToConfig);

        if (abstractTaskPartitionKeys == null) {
            config.setProperty("PROCESS_NAME", "pd");
            abstractTaskPartitionKeys = new PartitionDiscoveryTaskV2(config);
        }

        if (abstractTaskDeletedRows == null && config.getProperty("REPLICATE_DELETES").equals("true")) {
            config.setProperty("PROCESS_NAME", "drd");
            abstractTaskDeletedRows = new DeletedRowDiscoveryTask(config, meterRegistry);
        }

        if (config.getProperty("REPLICATE_DELETES").equals("true")) {
            dpdExecutor.execute(() -> {
                try {
                    abstractTaskDeletedRows.performTask(storageService, SYNC_DELETED_ROWS, countDownLatch);
                } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        pdExecutor.execute(() -> {
            try {
                abstractTaskPartitionKeys.performTask(storageService, SYNC_DELETED_PARTITION_KEYS, countDownLatch);
            } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });

        if (abstractTaskClusteringKeys == null) {
            config.setProperty("PROCESS_NAME", "rd");
            abstractTaskClusteringKeys = new RowDiscoveryTaskV2(config,
                    rowExecutor,
                    blockingQueue,
                    meterRegistry);
        }

        abstractTaskClusteringKeys.performTask(storageService,
                SYNC_CASSANDRA_ROWS,
                countDownLatch);

        countDownLatch.await();
        var elapsedTime = System.nanoTime() - startTime;
        LOGGER.info(
                "The replication process has completed within {} ms",
                Duration.ofNanos(elapsedTime).toMillis());
        var inserts = meterRegistry.get("replicated.insert").counter().count();
        var updates = meterRegistry.get("replicated.update").counter().count();
        var deletes = meterRegistry.get("replicated.delete").counter().count();

        if (inserts > 0) {
            LOGGER.info("Replicated inserts {}", inserts);
        } else {
            LOGGER.debug("Replicated inserts {}", inserts);
        }
        if (updates > 0) {
            LOGGER.info("Replicated updates {}", updates);
        } else {
            LOGGER.debug("Replicated updates {}", updates);
        }
        if (deletes > 0) {
            LOGGER.info("Replicated deletes {}", deletes);
        } else {
            LOGGER.debug("Replicated deletes {}", deletes);
        }
        return 0;
    }
}
