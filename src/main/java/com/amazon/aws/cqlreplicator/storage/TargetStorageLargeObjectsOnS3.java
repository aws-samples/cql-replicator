package com.amazon.aws.cqlreplicator.storage;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.amazon.aws.cqlreplicator.models.PrimaryKey;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TargetStorageLargeObjectsOnS3 extends TargetStorage<PrimaryKey, String> {

    private final Properties config;
    private final S3Client s3Client;

    public TargetStorageLargeObjectsOnS3(Properties config) {
        this.config = config;
        ConnectionFactory connectionFactory = new ConnectionFactory(config);
        this.s3Client = connectionFactory.buildS3Client();
    }

    @Override
    public void tearDown() {
        s3Client.close();
    }

    @Override
    public boolean write(String s) throws ExecutionException, InterruptedException, IOException {
        return false;
    }

    @Override
    public void delete(PrimaryKey o) throws IOException {

    }

    @Override
    public void delete(String o, PrimaryKey primaryKey) throws IOException {
        String key = "";

        if (config.getProperty("S3_SHARDING_BY").equals("PRIMARY_KEY")) {

            key = String.format("%s/%s/%s/%s/payload.json",
                            config.getProperty("TARGET_KEYSPACE"),
                            config.getProperty("TARGET_TABLE"),
                            primaryKey.getHashedPartitionKeys(),
                            primaryKey.getHashedClusteringKeys());


        } else if (config.getProperty("S3_SHARDING_BY").equals("NONE")) {

            key = String.format("%s/%s/%s/payload.json",
                            config.getProperty("TARGET_KEYSPACE"),
                            config.getProperty("TARGET_TABLE"),
                            primaryKey.getHashedPrimaryKey());
        }

        try {

            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(config.getProperty("S3_BUCKET_NAME"))
                    .key(key)
                    .build();

            s3Client.deleteObject(deleteObjectRequest);
           } catch (S3Exception e) {
            throw new RuntimeException(e);
        }
}

    @Override
    public boolean write(String s, PrimaryKey primaryKey) throws IOException {
        if (config.getProperty("S3_SHARDING_BY").equals("PRIMARY_KEY")) {
            var putObject = PutObjectRequest.builder()
                    .bucket(config.getProperty("S3_BUCKET_NAME"))
                    .key(String.format("%s/%s/%s/%s/payload.json",
                            config.getProperty("TARGET_KEYSPACE"),
                            config.getProperty("TARGET_TABLE"),
                            primaryKey.getHashedPartitionKeys(),
                            primaryKey.getHashedClusteringKeys())
                    )
                    .build();
            var res = s3Client.putObject(putObject, RequestBody.fromBytes(s.getBytes()));
            return res.sdkHttpResponse().isSuccessful();
        } else if (config.getProperty("S3_SHARDING_BY").equals("NONE")) {
            var putObject = PutObjectRequest.builder()
                    .bucket(config.getProperty("S3_BUCKET_NAME"))
                    .key(String.format("%s/%s/%s/payload.json",
                            config.getProperty("TARGET_KEYSPACE"),
                            config.getProperty("TARGET_TABLE"),
                            primaryKey.getHashedPrimaryKey()))
                    .build();
            var res = s3Client.putObject(putObject, RequestBody.fromBytes(s.getBytes()));
            return res.sdkHttpResponse().isSuccessful();
        }
        return false;

    }

}
