// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.storage.AdvancedCacheV2;
import com.amazon.aws.cqlreplicator.storage.SourceStorageOnCassandra;
import com.amazon.aws.cqlreplicator.storage.StorageServiceImpl;
import com.amazon.aws.cqlreplicator.task.AbstractTaskV2;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.amazon.aws.cqlreplicator.util.Utils.*;

/**
 * The {@code PartitionDiscoveryTask} class provides partition key synchronization between Cassandra
 * cluster and Amazon Keyspaces by using <em>token range split</em>. This implementation makes ~
 * &scanAndCompare; scan Cassandra cluster and compare any token range of <em>n</em>.
 *
 * <p>The {@code PartitionDiscoveryTask} splits Cassandra token range in <em>m</em> tiles and each
 * instance of PartitionDiscoveryTask handles only one tile
 *
 * <p>
 */

public class PartitionDiscoveryTaskV2 extends AbstractTaskV2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionDiscoveryTaskV2.class);
    private static final Pattern REGEX_PIPE = Pattern.compile("\\|");
    private static int ADVANCED_CACHE_SIZE;
    private static SourceStorageOnCassandra sourceStorageOnCassandra;
    private static Map<String, LinkedHashMap<String, String>> metaData;
    private final Properties config;

    /**
     * Constructor for PartitionDiscoveryTask.
     *
     * @param config the array to be sorted
     */

    public PartitionDiscoveryTaskV2(Properties config) {
        this.config = config;
        ADVANCED_CACHE_SIZE =
                Integer.parseInt(config.getProperty("LOCAL_STORAGE_PARTITIONS_PER_PAGE"));
        sourceStorageOnCassandra = new SourceStorageOnCassandra(config);
        metaData = sourceStorageOnCassandra.getMetaData();
    }

    /**
     * Scan and compare partition keys.
     */
    private void scanAndCompare(StorageServiceImpl storageService,
                                List<ImmutablePair<String, String>> rangeList, String[] pks)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {

        AdvancedCacheV2<String> advancedCache;
        List<Row> resultSetRange;

        advancedCache =
                new AdvancedCacheV2<>(ADVANCED_CACHE_SIZE, storageService) {
                    @Override
                    protected void flush(List<String> payload, StorageServiceImpl storage)
                            throws IOException {
                        var totalChunks = String.format("%s", "totalChunks");
                        var currentChunk = bytesToInt(storage.readTileMetadata(totalChunks));
                        LOGGER.debug("{}:{}", totalChunks, currentChunk);
                        var cborPayload = Utils.cborEncoder(payload);
                        var compressedCborPayload = Utils.compress(cborPayload);
                        var keyOfChunk =
                                String.format("%s|%s", "pksChunk", currentChunk);
                        storage.writePartitionsByChunk(keyOfChunk, compressedCborPayload);
                        storage.incrByOne(totalChunks);
                    }
                };

        boolean totalChunksExist =
                storageService.containsInPartitions(
                        String.format("%s", "totalChunks"));

        if (!totalChunksExist)
            storageService.writeTileMetadata(
                    String.format("%s", "totalChunks"),
                    intToBytes(0));

        var pksStr = String.join(",", pks);

        var partitioner = sourceStorageOnCassandra.getPartitioner();

        LOGGER.debug("Partitioner: {}", partitioner);

        for (ImmutablePair<String, String> range : rangeList) {

            Object rangeStart, rangeEnd;
            if (!partitioner.equals("org.apache.cassandra.dht.RandomPartitioner")) {
                rangeStart = Long.parseLong(range.left);
                rangeEnd = Long.parseLong(range.right);
                resultSetRange =
                        sourceStorageOnCassandra.findPartitionsByTokenRange(
                                pksStr, (Long) rangeStart, (Long) rangeEnd);

            } else {
                rangeStart = new BigInteger(range.left);
                rangeEnd = new BigInteger(range.right);
                resultSetRange =
                        sourceStorageOnCassandra.findPartitionsByTokenRange(
                                pksStr, (BigInteger) rangeStart, (BigInteger) rangeEnd);
            }

            LOGGER.trace("Processing a range: {} - {}", rangeStart, rangeEnd);
            for (Row eachResult : resultSetRange) {
                var i = 0;
                List<String> tmp = new ArrayList<>();

                for (String cl : pks) {
                    var type = metaData.get("partition_key").get(cl);
                    tmp.add(String.valueOf(eachResult.get(pks[i], Utils.getClassType(type.toUpperCase()))));
                    i++;
                }
                //TODO: Implement PartitionKeyV2
                var joinedPartitionKeys = String.join("|", tmp);
                var flag = storageService.containsInPartitions(joinedPartitionKeys);

                if (!flag) {
                    //TODO: Implement PartitionKeyV2
                    storageService.writePartition(
                            joinedPartitionKeys
                    );
                    //TODO: Implement PartitionKeyV2
                    advancedCache.put(joinedPartitionKeys);

                    LOGGER.debug("Syncing a new partition key: {}", joinedPartitionKeys);
                }
            }
        }

        if (advancedCache.getSize() > 0) {
            LOGGER.debug("Flushing remainders: {}", advancedCache.getSize());
            advancedCache.doFlush();
        }

        LOGGER.debug("Comparing stage is running");
    }

    //TODO: Implement PartitionKeyV2
    private void deletePartitions(String[] pks, StorageServiceImpl storageService, int chunk)
            throws IOException {

        var keyOfChunkFirst = String.format("%s|%s", "pksChunk", chunk);
        var compressedPayloadFirst = storageService.readPartitionsByChunk(keyOfChunkFirst);
        var cborPayloadFirst = Utils.decompress(compressedPayloadFirst);
        var collection = Utils.cborDecoder(cborPayloadFirst);
        //TODO: Implement PartitionKeyV2
        var finalClonedCollection = new CopyOnWriteArrayList<>(collection);
        collection.forEach(
                key -> {
                    BoundStatementBuilder boundStatementCassandraBuilder =
                            sourceStorageOnCassandra.getCassandraPreparedStatement().boundStatementBuilder();
                    //TODO: Implement PartitionKeyV2
                    LOGGER.debug("Processing partition key: {}", key);
                    //TODO: Implement PartitionKeyV2
                    var pk = REGEX_PIPE.split((String) key);

                    var i = 0;
                    //TODO: Implement PartitionKeyV2
                    for (String cl : pks) {
                        var type = metaData.get("partition_key").get(cl);
                        try {
                            boundStatementCassandraBuilder =
                                    Utils.aggregateBuilder(type, cl, pk[i], boundStatementCassandraBuilder);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        i++;
                    }

                    List<Row> cassandraResult =
                            sourceStorageOnCassandra.extract(boundStatementCassandraBuilder);

                    // Found deleted partition key
                    if (cassandraResult.size() == 0) {
                        LOGGER.debug("Found deleted partition key {}", key);

                        // Remove partition key from the cache
                        //TODO: Implement PartitionKeyV2
                        storageService.deletePartition(key.toString());
                        finalClonedCollection.remove(key);

                    }
                });

        if (finalClonedCollection.size() < collection.size()) {
            var cborPayload = Utils.cborEncoder(finalClonedCollection);
            var compressedPayload = Utils.compress(cborPayload);
            var keyOfChunk = String.format("%s|%s", "pksChunk", chunk);
            storageService.writePartitionsByChunk(keyOfChunk, compressedPayload);
        }

        if (finalClonedCollection.size() == 0) {
            var keyOfChunk = String.format("%s", "totalChunks");
            storageService.decrByOne(keyOfChunk);
        }
    }

    /**
     * Scan and remove deleted partition keys.
     *
     * @params rangeList, pkCache, pks the array to be sorted
     */
    //TODO: Implement PartitionKeyV2
    private void scanAndRemove(StorageServiceImpl storageService, Utils.CassandraTaskTypes taskName, String[] pks) throws IOException {

        if (taskName.equals(Utils.CassandraTaskTypes.SYNC_DELETED_PARTITION_KEYS)) {
            LOGGER.debug("Syncing deleted partition keys between C* and Amazon Keyspaces");
            var totalChunks = String.format("%s", "totalChunks");
            var chunks = bytesToInt(storageService.readTileMetadata(totalChunks));
            // remove each chunk of partition keys
            IntStream.range(0, chunks).forEach(
                    chunk -> {
                        try {
                            deletePartitions(pks, storageService, chunk);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        }
    }

    @Override
    protected void doPerformTask(StorageServiceImpl storageService, Utils.CassandraTaskTypes taskName,
                                 CountDownLatch countDownLatch)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {

        var pks = metaData.get("partition_key").keySet().toArray(new String[0]);
        LOGGER.debug("SOURCE_QUERY: {}", config.getProperty("SOURCE_CQL_QUERY"));

        List<ImmutablePair<String, String>> ranges = sourceStorageOnCassandra.getTokenRanges();
        var totalRanges = ranges.size();
        List<List<ImmutablePair<String, String>>> tiles =
                getDistributedRangesByTiles(ranges, Integer.parseInt(config.getProperty("TILES")));
        var currentTile = Integer.parseInt(config.getProperty("TILE"));
        List<ImmutablePair<String, String>> rangeList = tiles.get(currentTile);

        // if tiles = 0 we need to scan one range from one pkScanner, if tiles>0 we need to scan all
        // ranges from the pkScanner
        LOGGER.debug("The number of ranges in the cassandra: {}", totalRanges);
        LOGGER.debug("The number of ranges for the tile: {}", rangeList.size());
        LOGGER.debug("The number of tiles: {}", tiles.size());
        LOGGER.debug("The current tile: {}", currentTile);

        scanAndCompare(storageService, rangeList, pks);
        if (config.getProperty("REPLICATE_DELETES").equals("true")) {
            scanAndRemove(storageService, taskName, pks);
        }
        countDownLatch.countDown();
    }
}
