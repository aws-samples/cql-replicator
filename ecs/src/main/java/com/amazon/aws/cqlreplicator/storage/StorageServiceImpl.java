package com.amazon.aws.cqlreplicator.storage;

import com.amazon.aws.cqlreplicator.models.PrimaryKey;
import com.amazon.aws.cqlreplicator.util.LevelDBPageIterator;
import com.amazon.aws.cqlreplicator.util.Utils;
import org.apache.commons.lang3.SerializationUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.aws.cqlreplicator.util.Utils.bytesToInt;
import static com.amazon.aws.cqlreplicator.util.Utils.intToBytes;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class StorageServiceImpl implements StorageService<String, byte[], PrimaryKey>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageServiceImpl.class);
    private static final org.iq80.leveldb.Logger logger = LOGGER::info;
    private final int PAGE_SIZE;
    private final DB levelDBStorePartitions, levelDBStoreRows;

    public StorageServiceImpl(final Properties properties) throws IOException {
        this.PAGE_SIZE = Integer.parseInt(properties.getProperty("LOCAL_STORAGE_KEYS_PER_PAGE"));
        this.levelDBStorePartitions = initPartitionLevelStorage(properties);
        this.levelDBStoreRows = initRowLevelStorage(properties);
    }

    private static <T> byte[] addToCbor(byte[] cbor, T object) throws IOException {
        var set = Utils.cborDecoderSet(cbor);
        set.add(object);
        return Utils.cborEncoderSet(set);
    }

    private static <T> byte[] removeToCbor(byte[] cbor, T object) throws IOException {
        var set = Utils.cborDecoderSet(cbor);
        set.remove(object);
        return Utils.cborEncoderSet(set);
    }

    private DB initPartitionLevelStorage(final Properties properties) throws IOException {
        Options options = new Options();

        var LEVELDB_CACHE_SIZE = Integer.parseInt(properties.getProperty("LOCAL_STORAGE_CACHE_SIZE"));
        var LEVELDB_BLOCK_SIZE = Integer.parseInt(properties.getProperty("LOCAL_STORAGE_BLOCK_SIZE"));
        var LEVELDB_OPEN_MAX_FILES = Integer.parseInt(properties.getProperty("LOCAL_STORAGE_MAX_OPEN_FILES"));
        var LEVELDB_WRITE_BUFFER_SIZE = Integer.parseInt(properties.getProperty("LOCAL_STORAGE_WRITE_BUFFER_SIZE"));
        var LEVELDB_BLOCK_RESTART_INTERVAL = Integer.parseInt(properties.getProperty("LOCAL_STORAGE_BLOCK_RESTART_INTERVAL"));

        return
                factory.open(
                        new File(
                                String.format(
                                        "%s/ledger_v4_%s_partitions.ldb",
                                        properties.getProperty("LOCAL_STORAGE_PATH"),
                                        properties.getProperty("TILE"))),
                        options.cacheSize(LEVELDB_CACHE_SIZE).
                                blockSize(LEVELDB_BLOCK_SIZE).
                                maxOpenFiles(LEVELDB_OPEN_MAX_FILES).
                                writeBufferSize(LEVELDB_WRITE_BUFFER_SIZE).
                                blockRestartInterval(LEVELDB_BLOCK_RESTART_INTERVAL).
                                logger(logger).
                                verifyChecksums(true).
                                createIfMissing(true)
                );
    }

    private DB initRowLevelStorage(final Properties properties) throws IOException {
        Options options = new Options();
        var LEVELDB_CACHE_SIZE = Integer
                .parseInt(properties.getProperty("LOCAL_STORAGE_CACHE_SIZE"));
        var LEVELDB_BLOCK_SIZE = Integer
                .parseInt(properties.getProperty("LOCAL_STORAGE_BLOCK_SIZE"));
        var LEVELDB_OPEN_MAX_FILES = Integer
                .parseInt(properties.getProperty("LOCAL_STORAGE_MAX_OPEN_FILES"));
        var LEVELDB_WRITE_BUFFER_SIZE = Integer
                .parseInt(properties.getProperty("LOCAL_STORAGE_WRITE_BUFFER_SIZE"));
        var LEVELDB_BLOCK_RESTART_INTERVAL = Integer
                .parseInt(properties.getProperty("LOCAL_STORAGE_BLOCK_RESTART_INTERVAL"));

        return
                factory.open(
                        new File(
                                String.format(
                                        "%s/ledger_v4_%s_rows.ldb",
                                        properties.getProperty("LOCAL_STORAGE_PATH"),
                                        properties.getProperty("TILE"))),
                        options.cacheSize(LEVELDB_CACHE_SIZE).
                                blockSize(LEVELDB_BLOCK_SIZE).
                                maxOpenFiles(LEVELDB_OPEN_MAX_FILES).
                                writeBufferSize(LEVELDB_WRITE_BUFFER_SIZE).
                                blockRestartInterval(LEVELDB_BLOCK_RESTART_INTERVAL).
                                logger(logger).
                                verifyChecksums(true).
                                createIfMissing(true)
                );
    }

    @Override
    public void writePartition(String key) {
        var valueOnClient = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
        var value = SerializationUtils.serialize(valueOnClient);
        levelDBStorePartitions.put(SerializationUtils.serialize(key), value);
    }

    @Override
    public byte[] readPartition(String key) {
        return levelDBStorePartitions.get(SerializationUtils.serialize(key));
    }

    @Override
    public void deletePartition(String key) {
        levelDBStorePartitions.delete(SerializationUtils.serialize(key));
    }

    @Override
    public void writeRow(PrimaryKey key, byte[] bytes) {
        levelDBStoreRows.
                put(SerializationUtils.serialize(key), bytes);
    }

    @Override
    public byte[] readRow(PrimaryKey key) {
        return levelDBStoreRows.
                get(SerializationUtils.serialize(key));
    }

    @Override
    public void deleteRow(PrimaryKey key) {
        levelDBStoreRows.
                delete(SerializationUtils.serialize(key));
    }

    @Override
    public byte[] readPartitionsPerTile(String key) {
        return levelDBStorePartitions.get(SerializationUtils.serialize(key));
    }

    @Override
    public byte[] readTileMetadata(String key) {
        return levelDBStorePartitions.get(SerializationUtils.serialize(key));
    }

    @Override
    public void writeTileMetadata(String key, byte[] bytes) {
        levelDBStorePartitions.put(SerializationUtils.serialize(key), bytes);
    }

    // Changed here
    @Override
    public void writePartitionsByChunk(String key, byte[] data) {
        levelDBStorePartitions.put(SerializationUtils.serialize(key), data);
    }

    @Override
    public boolean containsInPartitions(String key) {
        return levelDBStorePartitions.get(SerializationUtils.serialize(key)) != null;
    }

    @Override
    public boolean containsInRows(PrimaryKey key) {
        return levelDBStoreRows
                .get(SerializationUtils.serialize(key)) != null;
    }

    // Changed here
    @Override
    public byte[] readPartitionsByChunk(String key) {
        return levelDBStorePartitions.get(SerializationUtils.serialize(key));
    }

    public void incrByOne(String key) {
        var primaryKey = SerializationUtils.serialize(key);
        var contains = levelDBStorePartitions.get(primaryKey) != null;
        if (!contains) {
            levelDBStorePartitions.put(primaryKey, Utils.intToBytes(0));
        }

        var currentValueAsIncrement = new AtomicInteger(bytesToInt(levelDBStorePartitions.get(primaryKey)));
        var newValueAsIncrement = currentValueAsIncrement.incrementAndGet();
        levelDBStorePartitions.put(primaryKey, intToBytes(newValueAsIncrement));
    }

    public void decrByOne(String key) {
        var primaryKey = SerializationUtils.serialize(key);
        var contains = levelDBStorePartitions.get(primaryKey) != null;
        if (!contains) {
            levelDBStorePartitions.put(primaryKey, intToBytes(0));
        }
        var currentValueAsIncrement = new AtomicInteger(bytesToInt(levelDBStorePartitions.get(primaryKey)));
        var newValueAsIncrement = currentValueAsIncrement.decrementAndGet();
        levelDBStorePartitions.put(primaryKey, intToBytes(newValueAsIncrement));
    }

    public Iterator<List<PrimaryKey>> readPaginatedPrimaryKeys() {
        return new LevelDBPageIterator(levelDBStoreRows, PAGE_SIZE);
    }

    public void closeIterator() throws IOException {
        levelDBStoreRows
                .iterator()
                .close();
    }

    @Override
    public void close() throws IOException {
        levelDBStorePartitions.close();
        levelDBStoreRows.close();
    }
}
