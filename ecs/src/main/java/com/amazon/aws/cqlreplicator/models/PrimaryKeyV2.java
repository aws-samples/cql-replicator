package com.amazon.aws.cqlreplicator.models;

import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class PrimaryKeyV2 {

    private static final XXHashFactory hashFactory = XXHashFactory.fastestInstance();
    private static final int hashSeed = 42;
    private final String[] partitionKeys;
    private final String[] clusteringKeys;
    private final int partitionKeySize;
    private final int clusteringColumnsSize;

    public PrimaryKeyV2(String[] partitionKeys, String[] clusteringColumns) {
        this.partitionKeySize = partitionKeys.length;
        this.clusteringColumnsSize = clusteringColumns.length;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = clusteringColumns;
    }

    public PrimaryKeyV2(String[] partitionKeys) {
        this.partitionKeySize = partitionKeys.length;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = new String[]{};
        this.clusteringColumnsSize = 0;
    }

    private static String[] deserializer(@NotNull ByteBuffer keys, @NotNull int numberOfKeys) {
        var decodedKey = new String[numberOfKeys];

        for (int i = 0; i < decodedKey.length; i++) {
            var strLen = keys.getInt();
            var strBytes = new byte[strLen];
            keys.get(strBytes);
            decodedKey[i] = new String(strBytes, StandardCharsets.UTF_8);
        }
        return decodedKey;
    }

    private static ByteBuffer serializer(@NotNull String[] keys) {
        var totalBytes = 0;
        for (var key : keys) {
            totalBytes += key.getBytes(StandardCharsets.UTF_8).length;
        }

        var buffer = ByteBuffer.allocate(totalBytes + keys.length * Integer.BYTES);

        for (var key : keys) {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            buffer.putInt(key.length());
            buffer.put(keyBytes);
        }

        buffer.rewind();
        return buffer;
    }

    private static long getXXHash(@NotNull String data) throws IOException {
        StreamingXXHash64 hash64 = hashFactory.newStreamingHash64(hashSeed);
        byte[] buffer = new byte[8192];
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
        for (; ; ) {
            int read = in.read(buffer);
            if (read == -1) {
                break;
            }
            hash64.update(buffer, 0, read);
        }
        return hash64.getValue();
    }

    private static String mergeKeys(@NotNull String[] keys) {
        var builder = new StringBuilder();
        for (String key : keys) {
            builder.append(key);
        }
        return builder.toString();
    }

    public ByteBuffer getSerializedPrimaryKey() {
        var mergedPks = new String[partitionKeySize + clusteringColumnsSize];
        System.arraycopy(partitionKeys, 0, mergedPks, 0, partitionKeySize);
        System.arraycopy(clusteringKeys, 0, mergedPks, partitionKeySize, clusteringColumnsSize);
        return serializer(mergedPks);
    }

    public String[] getPartitionKeys() {
        return partitionKeys;
    }

    public String[] getClusteringKeys() {
        return clusteringKeys;
    }

    public int getPartitionKeySize() {
        return partitionKeySize;
    }

    public int getClusteringColumnsSize() {
        return clusteringColumnsSize;
    }

    public ByteBuffer getSerializedPartitionKeys() {
        return serializer(partitionKeys);
    }

    public ByteBuffer getSerializedClusteringKeys() {
        return serializer(clusteringKeys);
    }

    public String[] getDeserializedPartitionKeys(@NotNull ByteBuffer key) {
        return deserializer(key, partitionKeySize);
    }

    public String[] getDeserializedClusteringKeys(@NotNull ByteBuffer key) {
        return deserializer(key, clusteringColumnsSize);
    }

    public long getXXHashPartitionKeys() throws IOException {
        return getXXHash(mergeKeys(partitionKeys));
    }

    public long getXXHashClusteringColumns() throws IOException {
        return getXXHash(mergeKeys(clusteringKeys));
    }

    public long getXXHashPrimaryKey() throws IOException {
        var mergedPks = new String[partitionKeySize + clusteringColumnsSize];
        System.arraycopy(partitionKeys, 0, mergedPks, 0, partitionKeySize);
        System.arraycopy(clusteringKeys, 0, mergedPks, partitionKeySize, clusteringColumnsSize);
        return getXXHash(mergeKeys(mergedPks));
    }

    @Override
    public boolean equals(@NotNull Object o) {
        if (this == o) return true;
        if (getClass() != o.getClass()) return false;
        PrimaryKeyV2 that = (PrimaryKeyV2) o;
        return Arrays.equals(partitionKeys, that.partitionKeys) &&
                Arrays.equals(clusteringKeys, that.clusteringKeys);
    }

}
