// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.models.Payload;
import com.amazon.aws.cqlreplicator.models.PrimaryKey;
import com.amazon.aws.cqlreplicator.models.StatsMetaData;
import com.amazon.aws.cqlreplicator.storage.IonEngine;
import com.amazon.aws.cqlreplicator.storage.SourceStorageOnCassandra;
import com.amazon.aws.cqlreplicator.storage.StorageServiceImpl;
import com.amazon.aws.cqlreplicator.storage.TargetStorageOnKeyspaces;
import com.amazon.aws.cqlreplicator.task.AbstractTaskV2;
import com.amazon.aws.cqlreplicator.util.CustomResultSetSerializer;
import com.amazon.aws.cqlreplicator.util.StatsCounter;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.shaded.guava.common.collect.MapDifference;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.amazon.aws.cqlreplicator.util.Utils.*;

/**
 * Responsible for replication logic between Cassandra and Amazon Keyspaces
 */
public class CassandraReplicationTaskV2 extends AbstractTaskV2 {

    public static final String CLUSTERING_COLUMN_ABSENT = "clusteringColumnAbsent";
    public static final String REPLICATION_NOT_APPLICABLE = "replicationNotApplicable";
    public static final Pattern REGEX_PIPE = Pattern.compile("\\|");
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraReplicationTaskV2.class);
    private static final int BLOCKING_QUEUE_SIZE = 15000;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final SimpleModule module = new SimpleModule();
    private static SourceStorageOnCassandra sourceStorageOnCassandra;
    private static TargetStorageOnKeyspaces targetStorageOnKeyspaces;
    private static Map<String, LinkedHashMap<String, String>> cassandraSchemaMetadata;
    private static StatsCounter statsCounter;
    private static Properties config = new Properties();
    private static int CORE_POOL_SIZE;
    private static int MAX_CORE_POOL_SIZE;
    private static int CORE_POOL_TIMEOUT;
    private static CloudWatchClient cloudWatchClient;
    private static boolean useCustomJsonSerializer = false;

    public CassandraReplicationTaskV2(final Properties cfg) {
        config = cfg;
        CORE_POOL_SIZE = Integer.parseInt(cfg.getProperty("REPLICATE_WITH_CORE_POOL_SIZE"));
        MAX_CORE_POOL_SIZE = Integer.parseInt(cfg.getProperty("REPLICATE_WITH_MAX_CORE_POOL_SIZE"));
        CORE_POOL_TIMEOUT = Integer.parseInt(cfg.getProperty("REPLICATE_WITH_CORE_POOL_TIMEOUT"));
        sourceStorageOnCassandra = new SourceStorageOnCassandra(config);
        cassandraSchemaMetadata = sourceStorageOnCassandra.getMetaData();
        statsCounter = new StatsCounter();
        targetStorageOnKeyspaces = new TargetStorageOnKeyspaces(config);
        useCustomJsonSerializer = !cfg.getProperty("SOURCE_CQL_QUERY").split(" ")[1].equalsIgnoreCase("json");
        if (useCustomJsonSerializer) {
            module.addSerializer(Row.class, new CustomResultSetSerializer());
            mapper.registerModule(module);
        }

        if (config.getProperty("ENABLE_CLOUD_WATCH").equals("true")) {
            try {
                cloudWatchClient =
                        CloudWatchClient.builder()
                                .region(Region.of(config.getProperty("CLOUD_WATCH_REGION")))
                                .build();
            } catch (CloudWatchException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected static String transformer(final String input, final Properties config) {
        if (config.getProperty("TRANSFORM_INBOUND_REQUEST").equals("true")) {
            var ionEngine = new IonEngine();
            var res = ionEngine.query(config.getProperty("TRANSFORM_SQL"), input);
            return res.substring(1, res.length() - 1);
        }
        return input;
    }

    private static String preparePayload(final Payload jsonPayload) {
        var ts = jsonPayload.getTimestamp();
        var query = "";
        var payload = transformer(jsonPayload.getPayload(), config);

        LOGGER.debug("PAYLOAD: {}", payload);
        // Prepare the JSON CQL statement with "USING TIMESTAMP" from the source
        if (config.getProperty("REPLICATE_WITH_TIMESTAMP").equals("true")) {
            query =
                    String.format(
                            doubleQuoteResolver("INSERT INTO %s.%s JSON '%s' USING TIMESTAMP %s", config.getProperty("SOURCE_CQL_QUERY")),
                            config.getProperty("TARGET_KEYSPACE"),
                            config.getProperty("TARGET_TABLE"),
                            payload,
                            ts);
        } else {
            query =
                    String.format(
                            doubleQuoteResolver("INSERT INTO %s.%s JSON '%s'", config.getProperty("SOURCE_CQL_QUERY")),
                            config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE"), payload);
        }
        return query;
    }

    private static void persistMetrics(StatsMetaData statsMetadata) {
        if (statsMetadata.getValue() > 0) {
            if (config.getProperty("ENABLE_CLOUD_WATCH").equals("false")) {
                targetStorageOnKeyspaces.writeStats(statsMetadata);
            } else {
                var metricVal = statsMetadata.getValue();
                var metricName = statsMetadata.getOps();
                putMetricData(cloudWatchClient, Double.valueOf(String.valueOf(metricVal)), metricName);
            }
        }
    }

    private static String getSerializedCassandraRow(Row row) throws JsonProcessingException {
        String payload;
        if (useCustomJsonSerializer) {
            payload = mapper.writeValueAsString(row).replace("\\\"writetime(", "writetime(").replace(")\\\"", ")");
        } else {
            payload = row.getString(0);
        }
        return payload;
    }

    private void delete(
            final PrimaryKey primaryKey,
            final String[] pks,
            final String[] cls,
            StorageServiceImpl storageService)
            throws ArrayIndexOutOfBoundsException {
        if (!sourceStorageOnCassandra.findPrimaryKey(primaryKey, pks, cls)) {
            var rowIsDeleted =
                    targetStorageOnKeyspaces.delete(primaryKey, pks, cls, cassandraSchemaMetadata);
            if (rowIsDeleted) {
                storageService.deleteRow(primaryKey);
                statsCounter.incrementStat("DELETE");
            }
        }
    }

    private void replicateDeletedCassandraRow(
            final String[] pks, final String[] cls, StorageServiceImpl storageService) {

        var ledger = storageService.readPaginatedPrimaryKeys();
        ledger.forEachRemaining(
                primaryKeys ->
                        primaryKeys.parallelStream()
                                .forEach(
                                        pk ->
                                                delete(pk, pks, cls, storageService)
                                ));
    }

    @Override
    protected void doPerformTask(StorageServiceImpl storageService, CassandraTaskTypes taskName)
            throws InterruptedException {

        var partitionKeyNames =
                cassandraSchemaMetadata.get("partition_key").keySet().toArray(new String[0]);

        var clusteringColumnNames =
                cassandraSchemaMetadata.get("clustering").keySet().toArray(new String[0]);

        BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_SIZE);

        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        CORE_POOL_SIZE,
                        MAX_CORE_POOL_SIZE,
                        CORE_POOL_TIMEOUT,
                        TimeUnit.SECONDS,
                        blockingQueue,
                        new ThreadPoolExecutor.AbortPolicy());

        var chunks = Utils.bytesToInt(storageService.readTileMetadata(String.format("%s", "totalChunks")));

        if (chunks > 0) {
            var stream = IntStream.range(0, chunks);
            stream
                    .parallel()
                    .forEach(
                            chunk -> {
                                List<Object> listOfPartitionKeys;
                                try {
                                    var keyOfChunkFirst = String.format("%s|%s", "pksChunk", chunk);
                                    var compressedPayloadFirst = storageService.readPartitionsByChunk(keyOfChunkFirst);
                                    var cborPayloadFirst = Utils.decompress(compressedPayloadFirst);
                                    listOfPartitionKeys =
                                            Utils.cborDecoder(cborPayloadFirst);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                assert listOfPartitionKeys != null;

                                executor.prestartAllCoreThreads();
                                listOfPartitionKeys.forEach(
                                        row ->
                                                blockingQueue.offer(
                                                        new RowReplicationTask(
                                                                partitionKeyNames,
                                                                clusteringColumnNames,
                                                                row.toString(),
                                                                storageService)));
                            });

        }

        if (config.getProperty("REPLICATE_DELETES").equals("true")) {
            var startTime = System.nanoTime();
            replicateDeletedCassandraRow(partitionKeyNames, clusteringColumnNames, storageService);
            var elapsedTime = System.nanoTime() - startTime;
            LOGGER.debug(
                    "Replicate deletes takes: {}", Duration.ofNanos(elapsedTime).toMillis());

        }

        var statsMetaDataInserts =
                new StatsMetaData(
                        Integer.parseInt(config.getProperty("TILE")),
                        config.getProperty("TARGET_KEYSPACE"),
                        config.getProperty("TARGET_TABLE"),
                        "INSERT");

        statsMetaDataInserts.setValue(statsCounter.getStat("INSERT"));
        persistMetrics(statsMetaDataInserts);

        var statsMetaDataUpdates =
                new StatsMetaData(
                        Integer.parseInt(config.getProperty("TILE")),
                        config.getProperty("TARGET_KEYSPACE"),
                        config.getProperty("TARGET_TABLE"),
                        "UPDATE");

        statsMetaDataUpdates.setValue(statsCounter.getStat("UPDATE"));
        persistMetrics(statsMetaDataUpdates);

        var statsMetaDataDeletes =
                new StatsMetaData(
                        Integer.parseInt(config.getProperty("TILE")),
                        config.getProperty("TARGET_KEYSPACE"),
                        config.getProperty("TARGET_TABLE"),
                        "DELETE");

        statsMetaDataDeletes.setValue(statsCounter.getStat("DELETE"));
        persistMetrics(statsMetaDataDeletes);

        statsCounter.resetStat("INSERT");
        statsCounter.resetStat("UPDATE");
        statsCounter.resetStat("DELETE");

        executor.shutdown();
        executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public static class RowReplicationTask implements Runnable {

        private static StorageServiceImpl storageService;
        private final String[] pks;
        private final String[] cls;
        private final String partitionKey;

        public RowReplicationTask(
                final String[] pks,
                final String[] cls,
                final String pk,
                StorageServiceImpl pkc) {
            this.pks = pks;
            this.cls = cls;
            this.partitionKey = pk;
            storageService = pkc;
        }

        public static BoundStatementBuilder prepareCassandraStatement(String[] pk, String[] pks) {
            BoundStatementBuilder boundStatementCassandraBuilder =
                    sourceStorageOnCassandra.getCassandraPreparedStatement().boundStatementBuilder();
            int i = 0;
            try {
                for (String columnName : pks) {
                    var type = cassandraSchemaMetadata.get("partition_key").get(columnName);
                    boundStatementCassandraBuilder =
                            aggregateBuilder(type, columnName, pk[i], boundStatementCassandraBuilder);
                    i++;
                }
            } catch (ArrayIndexOutOfBoundsException exception) {
                LOGGER.error("Incorrect pk {} in a position {}", Arrays.toString(pk), i);
                LOGGER.error("Reason: perhaps one of the column in the primary key is an empty string");
                throw new RuntimeException(exception);
            }
            return boundStatementCassandraBuilder;
        }

        private void dataLoader(
                final SimpleStatement simpleStatement,
                final long lastWriteTimestamp,
                final boolean setStartReplicationPoint,
                final long ts,
                final String ops) {

            if (!setStartReplicationPoint) {
                var result = targetStorageOnKeyspaces.write(simpleStatement);
                if (result) {
                    statsCounter.incrementStat(ops);
                }

            } else {
                if (lastWriteTimestamp > ts) {
                    var result = targetStorageOnKeyspaces.write(simpleStatement);
                    if (result) {
                        statsCounter.incrementStat(ops);
                    }
                }
            }
        }

        private void insertRow(
                final long v,
                final String k,
                final ConcurrentMap<String, String> jsonColumnHashMapPerPartition) {
            var valueOnClient = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
            var simpleStatement =
                    SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                            .setIdempotent(true)
                            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

            dataLoader(
                    simpleStatement,
                    valueOnClient,
                    Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
                    Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")),
                    "INSERT");
        }

        private void updateRow(
                final MapDifference.ValueDifference<Long> v,
                final String k,
                final ConcurrentMap<String, String> jsonColumnHashMapPerPartition) {
            var valueOnClient = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
            if (v.leftValue() > v.rightValue()) {
                var simpleStatement =
                        SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                                .setIdempotent(true)
                                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

                dataLoader(
                        simpleStatement,
                        valueOnClient,
                        Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
                        Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")),
                        "UPDATE");
            }
        }

        @Override
        public void run() {

            var pk = REGEX_PIPE.split(partitionKey);
            ConcurrentMap<String, Long> ledgerHashMap = new ConcurrentHashMap<>();
            ConcurrentMap<String, Long> sourceHashMap = new ConcurrentHashMap<>();
            ConcurrentMap<String, String> jsonColumnHashMapPerPartition = new ConcurrentHashMap<>();

            var boundStatementCassandraBuilder = prepareCassandraStatement(pk, pks);

            var cassandraResult = sourceStorageOnCassandra.extract(boundStatementCassandraBuilder);
            cassandraResult.parallelStream()
                    .forEach(
                            compareRow -> {
                                Payload jsonPayload = null;
                                try {
                                    jsonPayload = Utils.convertToJson(
                                            getSerializedCassandraRow(compareRow),
                                            config.getProperty("WRITETIME_COLUMNS"),
                                            cls,
                                            pks);
                                } catch (JsonProcessingException e) {
                                    throw new RuntimeException(e);
                                }
                                var ts = jsonPayload.getTimestamp();
                                Map<String, String> clusteringColumnsMapping = jsonPayload.getClusteringColumns();
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
                                PrimaryKey primaryKey = null;
                                try {
                                    primaryKey = new PrimaryKey(partitionKey, cl);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }

                                // if hk is not in the global pk cache, add it
                                if (!storageService.containsInRows(primaryKey)) {
                                    sourceHashMap.put(cl, ts);
                                    storageService.writeRow(primaryKey, longToBytes(ts));
                                    jsonColumnHashMapPerPartition.put(cl, preparePayload(jsonPayload));
                                } else {
                                    // if hk is in the global pk cache, compare timestamps
                                    if (ts > bytesToLong(storageService.readRow(primaryKey))) {
                                        sourceHashMap.put(cl, ts);
                                        storageService.writeRow(primaryKey, longToBytes(ts));
                                        var query = preparePayload(jsonPayload);
                                        jsonColumnHashMapPerPartition.put(cl, preparePayload(jsonPayload));
                                    }
                                }
                            });

            MapDifference<String, Long> diff = Maps.difference(sourceHashMap, ledgerHashMap);
            Map<String, MapDifference.ValueDifference<Long>> rowDiffering = diff.entriesDiffering();
            Map<String, Long> newItems = diff.entriesOnlyOnLeft();

            rowDiffering.forEach(
                    (k, v) ->
                            updateRow(v, k, jsonColumnHashMapPerPartition)
            );

            newItems.forEach(
                    (k, v) ->
                            insertRow(v, k, jsonColumnHashMapPerPartition)
            );
        }
    }
}
