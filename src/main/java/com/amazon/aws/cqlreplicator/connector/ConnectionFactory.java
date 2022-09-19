/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.connector;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import net.spy.memcached.MemcachedClient;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

public class ConnectionFactory {

  private Properties config;

  public ConnectionFactory(Properties config) {
    this.config = config;
  }

  public CqlSession buildCqlSession(String applicationConfName) {
    final var configFile =
        new File(String.format("%s/%s", config.getProperty("PATH_TO_CONFIG"), applicationConfName));

    return CqlSession.builder()
        .withConfigLoader(DriverConfigLoader.fromFile(configFile))
        .addTypeCodecs(TypeCodecs.ZONED_TIMESTAMP_UTC)
        .build();
  }

  public MemcachedClient buildMemcachedSession() throws IOException {
    return new MemcachedClient(
        new InetSocketAddress(
            config.getProperty("EXTERNAL_MEMCACHED_STORAGE_ENDPOINT"),
            Integer.parseInt(config.getProperty("EXTERNAL_MEMCACHED_STORAGE_PORT"))));
  }
}
