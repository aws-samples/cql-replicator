package com.amazon.aws.cqlreplicator.storage;

import java.io.IOException;

public interface StorageService<K, V, O> {

    void writePartition(K k) throws IOException;

    V readPartition(K k) throws IOException;

    void deletePartition(K k) throws IOException;

    void writeRow(O k, V v) throws IOException;

    V readRow(O k) throws IOException;

    void deleteRow(O k) throws IOException;

    V readPartitionsPerTile(K k) throws IOException;

    V readTileMetadata(K k) throws IOException;

    void writeTileMetadata(K k, V v) throws IOException;

    void writePartitionsByChunk(K k, V v) throws IOException;

    boolean containsInPartitions(K k) throws IOException;

    boolean containsInRows(O k) throws IOException;

    V readPartitionsByChunk(K k) throws IOException;

}
