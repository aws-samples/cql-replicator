// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * ConfigReader is the class that defines the reading process from config.yaml file by using
 * YAMLFactory().
 */
public class ConfigReader {

  private static final String configName = "config.properties";
  private final String pathToConfig;

  public ConfigReader(String pathToConfig) {
    this.pathToConfig = pathToConfig;
  }

  public Properties getConfig() throws IOException {

    Properties properties = new Properties();
    properties.load(new FileInputStream(String.format("%s/%s", pathToConfig, configName)));

    return properties;
  }
}
