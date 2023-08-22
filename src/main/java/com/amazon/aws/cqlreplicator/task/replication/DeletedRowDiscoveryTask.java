package com.amazon.aws.cqlreplicator.task.replication;

import com.amazon.aws.cqlreplicator.models.PrimaryKey;
import com.amazon.aws.cqlreplicator.storage.SourceStorageOnCassandra;
import com.amazon.aws.cqlreplicator.storage.StorageServiceImpl;
import com.amazon.aws.cqlreplicator.storage.TargetStorageLargeObjectsOnS3;
import com.amazon.aws.cqlreplicator.storage.TargetStorageOnKeyspaces;
import com.amazon.aws.cqlreplicator.task.AbstractTaskV2;
import com.amazon.aws.cqlreplicator.util.Utils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class DeletedRowDiscoveryTask extends AbstractTaskV2 {
    private static TargetStorageLargeObjectsOnS3 targetStorageLargeObjectsOnS3;
    private static Map<String, LinkedHashMap<String, String>> cassandraSchemaMetadata;
    private static SourceStorageOnCassandra sourceStorageOnCassandra;
    private static TargetStorageOnKeyspaces targetStorageOnKeyspaces;
    private static final Logger LOGGER = LoggerFactory.getLogger(DeletedRowDiscoveryTask.class);
    private static Properties config;
    private static MeterRegistry meterRegistry;
    private static Counter cntDeletes;

    public DeletedRowDiscoveryTask(Properties config, MeterRegistry meterRegistry) {
        sourceStorageOnCassandra = new SourceStorageOnCassandra(config);
        cassandraSchemaMetadata = sourceStorageOnCassandra.getMetaData();
        targetStorageOnKeyspaces = new TargetStorageOnKeyspaces(config);
        DeletedRowDiscoveryTask.config = config;
        DeletedRowDiscoveryTask.meterRegistry = meterRegistry;
        cntDeletes = Counter.builder("replicated.delete")
                .description("Replicated deletes counter")
                .register(meterRegistry);
        if (!config.getProperty("S3_OFFLOAD_COLUMNS").equals("NONE")) {
            DeletedRowDiscoveryTask.targetStorageLargeObjectsOnS3 = new TargetStorageLargeObjectsOnS3(config);
        }
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
                cntDeletes.increment();
                if (!config.getProperty("S3_OFFLOAD_COLUMNS").equals("NONE")) {
                    try {
                        targetStorageLargeObjectsOnS3.delete("", primaryKey);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
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
    protected void doPerformTask(StorageServiceImpl storageService, Utils.CassandraTaskTypes taskName,
                                 CountDownLatch countDownLatch)  {
        var partitionKeyNames =
                cassandraSchemaMetadata.get("partition_key").keySet().toArray(new String[0]);

        var clusteringColumnNames =
                cassandraSchemaMetadata.get("clustering").keySet().toArray(new String[0]);

        replicateDeletedCassandraRow(partitionKeyNames, clusteringColumnNames, storageService);

        countDownLatch.countDown();
    }
}
