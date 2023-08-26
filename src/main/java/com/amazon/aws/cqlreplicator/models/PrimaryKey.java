/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.models;

import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;

public class PrimaryKey implements Serializable, Comparable<PrimaryKey> {
    private static final long serialVersionUID = 1L;
    private static final XXHashFactory hashFactory = XXHashFactory.fastestInstance();
    private static final int hashSeed = 42;
    private final String partitionKeys;
    private final long hash;
    private final String clusteringColumns;
    public PrimaryKey(String partitionKeys, String clusteringColumns) throws IOException {
        this.partitionKeys = partitionKeys;
        this.clusteringColumns = clusteringColumns;
        this.hash = getXXHash(String.format("%s%s", partitionKeys, clusteringColumns));
    }

    public long getHashedPartitionKeys() throws IOException {
        return getXXHash(partitionKeys);
    }

    public long getHashedPrimaryKey() throws IOException {
        return getXXHash(String.format("%s|%s", partitionKeys, clusteringColumns));

    }

    public long getHashedClusteringKeys() throws IOException {
        return getXXHash(clusteringColumns);
    }

    private static long getXXHash(@NotNull String data) throws IOException {
        try (StreamingXXHash64 hash64 = hashFactory.newStreamingHash64(hashSeed)) {
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
    }

    public long getHash() {
        return hash;
    }

    public String getPartitionKeys() {
        return partitionKeys;
    }

    public String getClusteringColumns() {
        return clusteringColumns;
    }

    @Override
    public String toString() {
        return "PrimaryKey{" +
                "partitionKeys='" + partitionKeys + '\'' +
                ", clusteringColumns='" + clusteringColumns + '\'' +
                '}';
    }

    @Override
    public int compareTo(@NotNull PrimaryKey o) {
        var c1 = this.getPartitionKeys().compareTo(o.getPartitionKeys());
        var c2 = this.getClusteringColumns().compareTo(o.getClusteringColumns());
        if (c1 == 0 && c2 == 0)
            return 0;
        else
            return -1;
    }

}
