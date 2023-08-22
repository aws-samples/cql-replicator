// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.models.Payload;
import com.amazon.aws.cqlreplicator.models.PrimaryKey;
import com.amazon.aws.cqlreplicator.storage.*;
import com.amazon.aws.cqlreplicator.task.AbstractTaskV2;
import com.amazon.aws.cqlreplicator.util.CustomResultSetSerializer;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.amazon.aws.cqlreplicator.util.Utils.*;

/**
 * Responsible for replication logic between Cassandra and Amazon Keyspaces
 */
public class RowDiscoveryTaskV2 extends AbstractTaskV2 {

    public static final String CLUSTERING_COLUMN_ABSENT = "clusteringColumnAbsent";
    public static final String REPLICATION_NOT_APPLICABLE = "replicationNotApplicable";
    public static final Pattern REGEX_PIPE = Pattern.compile("\\|");
    private static final Logger LOGGER = LoggerFactory.getLogger(RowDiscoveryTaskV2.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final SimpleModule module = new SimpleModule();
    private static SourceStorageOnCassandra sourceStorageOnCassandra;
    private static TargetStorageOnKeyspaces targetStorageOnKeyspaces;
    private static Map<String, LinkedHashMap<String, String>> cassandraSchemaMetadata;
    private static Properties config = new Properties();
    private static boolean useCustomJsonSerializer = false;
    private final ThreadPoolExecutor executor;
    private final BlockingQueue<Runnable> blockingQueue;
    private static MeterRegistry meterRegistry;
    private static Counter cntInserts;
    private static Counter cntUpdates;
    private static Counter cntFailedInserts;
    private static Counter cntFailedUpdates;
    private static TargetStorageLargeObjectsOnS3 targetStorageLargeObjectsOnS3;

    public RowDiscoveryTaskV2(final Properties cfg,
                              ThreadPoolExecutor executor,
                              BlockingQueue<Runnable> blockingQueue,
                              MeterRegistry meterRegistry) {
        this.executor = executor;
        this.blockingQueue = blockingQueue;
        RowDiscoveryTaskV2.meterRegistry = meterRegistry;
        cntInserts = Counter.builder("replicated.insert")
                .description("Replicated inserts counter")
                .register(meterRegistry);
        cntUpdates = Counter.builder("replicated.update")
                .description("Replicated updates counter")
                .register(meterRegistry);
        cntFailedInserts = Counter.builder("failed.insert")
                .description("Failed inserts counter")
                .register(meterRegistry);
        cntFailedUpdates = Counter.builder("failed.update")
                .description("Failed updates counter")
                .register(meterRegistry);
        config = cfg;
        sourceStorageOnCassandra = new SourceStorageOnCassandra(config);
        cassandraSchemaMetadata = sourceStorageOnCassandra.getMetaData();
        targetStorageOnKeyspaces = new TargetStorageOnKeyspaces(config);
        useCustomJsonSerializer = !cfg.getProperty("SOURCE_CQL_QUERY").split(" ")[1].equalsIgnoreCase("json");
        if (useCustomJsonSerializer) {
            module.addSerializer(Row.class, new CustomResultSetSerializer());
            mapper.registerModule(module);
        }
        if (!config.getProperty("S3_OFFLOAD_COLUMNS").equals("NONE")) {
            RowDiscoveryTaskV2.targetStorageLargeObjectsOnS3 = new TargetStorageLargeObjectsOnS3(config);
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

    private static void reportTelemetry(String ops, boolean isSuccessful) {
        if (!isSuccessful) {
            if (ops.equals("INSERT"))  {
                cntFailedInserts.increment();
            } else if (ops.equals("UPDATE")) {
                cntFailedUpdates.increment();
            }
            } else {
            if (ops.equals("INSERT"))  {
                cntInserts.increment();
            } else if (ops.equals("UPDATE")) {
                cntUpdates.increment();
            }
        }
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

    private static String getSerializedCassandraRow(Row row) throws JsonProcessingException {
        String payload;
        if (useCustomJsonSerializer) {
            payload = mapper.writeValueAsString(row).replace("\\\"writetime(", "writetime(").replace(")\\\"", ")");
        } else {
            payload = row.getString(0);
        }
        return payload;
    }

    @Override
    protected void doPerformTask(StorageServiceImpl storageService,
                                 CassandraTaskTypes taskName, CountDownLatch countDownLatch) {

        var partitionKeyNames =
                cassandraSchemaMetadata.get("partition_key").keySet().toArray(new String[0]);

        var clusteringColumnNames =
                cassandraSchemaMetadata.get("clustering").keySet().toArray(new String[0]);

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
                                                                storageService))
                                        );
                            });

        }

        LOGGER.debug("Get the pool size: {}", executor.getPoolSize());
        LOGGER.debug("Get active threads count  {}",  executor.getActiveCount());
        LOGGER.debug("Get task count {}", executor.getTaskCount());
        LOGGER.debug("Get completed task count {}", executor.getCompletedTaskCount());
        LOGGER.debug("Get Remaining queue capacity {}", blockingQueue.remainingCapacity());

        countDownLatch.countDown();
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

        private static void dataLoader(
                final SimpleStatement simpleStatement,
                final long lastWriteTimestamp,
                final boolean setStartReplicationPoint,
                final long ts,
                final String ops) {

            if (!setStartReplicationPoint) {
                var result = targetStorageOnKeyspaces.write(simpleStatement);
                reportTelemetry(ops, result);

            } else {
                if (lastWriteTimestamp > ts) {
                    var result = targetStorageOnKeyspaces.write(simpleStatement);
                    reportTelemetry(ops, result);
                }
            }
        }

        private static void upsertRow(
                final long v,
                final String k,
                final ConcurrentMap<String, String> jsonColumnHashMapPerPartition,
                final String ops) {
            var simpleStatement =
                    SimpleStatement.newInstance(jsonColumnHashMapPerPartition.get(k))
                            .setIdempotent(true)
                            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

            dataLoader(
                    simpleStatement,
                    v,
                    Boolean.parseBoolean(config.getProperty("ENABLE_REPLICATION_POINT")),
                    Long.parseLong(config.getProperty("STARTING_REPLICATION_TIMESTAMP")),
                    ops
                    );
        }

        @Override
        public void run() {

            var pk = REGEX_PIPE.split(partitionKey);
            ConcurrentMap<String, String> jsonColumnHashMapPerPartition = new ConcurrentHashMap<>();

            var boundStatementCassandraBuilder = prepareCassandraStatement(pk, pks);

            var cassandraResult = sourceStorageOnCassandra.extract(boundStatementCassandraBuilder);
            cassandraResult.parallelStream()
                    .forEach(
                            compareRow -> {
                                Payload jsonPayload;
                                try {
                                    jsonPayload = Utils.convertToJson(
                                            getSerializedCassandraRow(compareRow),
                                            config.getProperty("WRITETIME_COLUMNS"),
                                            cls,
                                            pks,
                                            config);
                                } catch (IOException e) {
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
                                PrimaryKey primaryKey;
                                try {
                                    primaryKey = new PrimaryKey(partitionKey, cl);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }

                                // if hk is not in the global pk cache, add it
                                if (!storageService.containsInRows(primaryKey)) {
                                    storageService.writeRow(primaryKey, longToBytes(ts));
                                    jsonColumnHashMapPerPartition.put(cl, preparePayload(jsonPayload));
                                    upsertRow(ts, cl, jsonColumnHashMapPerPartition, "INSERT");
                                    if (!config.getProperty("S3_OFFLOAD_COLUMNS").equals("NONE")) {
                                        try {
                                            targetStorageLargeObjectsOnS3.write(jsonPayload.getS3Payload() , primaryKey);
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }

                                } else {
                                    // if hk is in the global pk cache, compare timestamps
                                    if (ts > bytesToLong(storageService.readRow(primaryKey))) {
                                        storageService.writeRow(primaryKey, longToBytes(ts));
                                        jsonColumnHashMapPerPartition.put(cl, preparePayload(jsonPayload));
                                        upsertRow(ts, cl, jsonColumnHashMapPerPartition, "UPDATE");
                                        if (!config.getProperty("S3_OFFLOAD_COLUMNS").equals("NONE")) {
                                            try {
                                                targetStorageLargeObjectsOnS3.write(jsonPayload.getS3Payload(), primaryKey);
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }
                                    }
                                }
                            });
        }
    }
}
