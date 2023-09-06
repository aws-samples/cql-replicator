/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.connector;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;

public class ConnectionFactory {

    private final Properties config;

    public ConnectionFactory(final @NotNull Properties config) {
        this.config = config;
    }

    public CqlSession buildCqlSession(final @NotNull String applicationConfName) {
        final var configFile =
                new File(String.format("%s/%s", config.getProperty("PATH_TO_CONFIG"), applicationConfName));

        return CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(configFile))
                .addTypeCodecs(TypeCodecs.ZONED_TIMESTAMP_UTC)
                .build();
    }

    public S3Client buildS3Client() {
        var clientOverrideConfiguration =
                ClientOverrideConfiguration.builder()
                        .apiCallAttemptTimeout(Duration.ofSeconds(2))
                        .retryPolicy(RetryPolicy.builder().backoffStrategy(BackoffStrategy.defaultStrategy()).numRetries(64).build())
                        .build();

        return S3Client.builder()
                .overrideConfiguration(clientOverrideConfiguration)
                .region(Region.of(config.getProperty("S3_REGION")))
                .endpointOverride(URI.create(
                        String.format("https://s3.%s.amazonaws.com", config.getProperty("S3_REGION"))))
                .forcePathStyle(true)
                .build();
    }
}
