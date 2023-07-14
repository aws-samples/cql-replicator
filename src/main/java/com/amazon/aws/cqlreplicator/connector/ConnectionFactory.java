/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.connector;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import org.jetbrains.annotations.NotNull;

import java.io.File;
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

}
