// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** Responsible for initiating the CQLReplicator's tables in Amazon Keyspaces */
public class Init {

  private static final String[] dropOps =
      new String[] {
        "DROP KEYSPACE IF EXISTS replicator", "DROP KEYSPACE IF EXISTS ks_test_cql_replicator"
      };

  private static final String[] createKeyspaceOps =
      new String[] {
        "CREATE KEYSPACE IF NOT EXISTS replicator WITH replication = "
            + "{'class': 'SimpleStrategy', 'replication_factor': 1 }",
        "CREATE KEYSPACE IF NOT EXISTS ks_test_cql_replicator WITH replication = "
            + "{'class': 'SimpleStrategy', 'replication_factor': 1 }"
      };

  private static final String[] createTblTestOps =
      new String[] {
        "CREATE TABLE ks_test_cql_replicator.test_cql_replicator (\n"
            + "    key uuid,\n"
            + "    col0 tinyint,\n"
            + "    col1 text,\n"
            + "    col2 date,\n"
            + "    col3 double,\n"
            + "    col4 int,\n"
            + "    col5 bigint,\n"
            + "    col6 timestamp,\n"
            + "    col7 float,\n"
            + "    col8 blob,\n"
            + "    PRIMARY KEY (key, col0)\n"
            + ") WITH CLUSTERING ORDER BY (col0 ASC)"
      };

  private static final String[] createTblReplicatorOps =
      new String[] {
        "CREATE TABLE replicator.ledger_v4 (\n"
            + "process_name text,\n"
            + "tile int,\n"
            + "keyspacename text,\n"
            + "tablename text,\n"
            + "pk text,\n"
            + "cc text,\n"
            + "operation_ts timestamp,\n"
            + "value bigint,\n"
            + "PRIMARY KEY ((process_name, tile, keyspacename, tablename, pk), cc)\n"
            + ") WITH CLUSTERING ORDER BY (cc ASC)\n",
        "CREATE TABLE IF NOT EXISTS replicator.retries (\n"
            + "    dt_retries date,\n"
            + "    pk text,\n"
            + "    cl text,\n"
            + "    ops text,\n"
            + "    keyspacename text,\n"
            + "    tablename text,\n"
            + "    cnt counter,\n"
            + "    PRIMARY KEY ((dt_retries, pk, cl), ops, keyspacename, tablename)\n"
            + ") WITH CLUSTERING ORDER BY (ops ASC, keyspacename ASC, tablename ASC)",
        "CREATE TABLE IF NOT EXISTS replicator.stats (\n"
            + "    tile int,\n"
            + "    keyspacename text,\n"
            + "    tablename text,\n"
            + "    ops text,\n"
            + "    \"rows\" counter,\n"
            + "    PRIMARY KEY ((tile, keyspacename, tablename, ops)))",
        "CREATE TABLE replicator.deleted_partitions (\n"
            + "    tile int,\n"
            + "    pk text,\n"
            + "    keyspacename text,\n"
            + "    tablename text,\n"
            + "    deleted_ts bigint,\n"
            + "    PRIMARY KEY ((tile, pk, keyspacename, tablename), deleted_ts)\n"
            + ") WITH CLUSTERING ORDER BY (deleted_ts ASC)"
      };
  private static final String pathToConfig = System.getenv("CQLREPLICATOR_CONF");
  private static final String KEYSPACES_CONFIG_FILE = "KeyspacesConnector.conf";
  private static final File configFile =
      new File(String.format("%s/%s", pathToConfig, KEYSPACES_CONFIG_FILE));
  private static CountDownLatch latch = new CountDownLatch(5);
  private static CqlSession cqlSession =
      CqlSession.builder()
          .addSchemaChangeListener(listener)
          .withConfigLoader(DriverConfigLoader.fromFile(configFile))
          .addTypeCodecs(TypeCodecs.ZONED_TIMESTAMP_UTC)
          .build();
  private static final SchemaChangeListener listener =
      new SchemaChangeListenerBase() {
        @Override
        public void onKeyspaceCreated(@NotNull KeyspaceMetadata keyspace) {
          super.onKeyspaceCreated(keyspace);
          if (keyspace.getName().asCql(true).equals("replicator")) {
            for (String ops : createTblReplicatorOps) {
              cqlSession.executeAsync(ops);
              latch.countDown();
            }
          }
          if (keyspace.getName().asCql(true).equals("ks_test_cql_replicator")) {
            for (String ops : createTblTestOps) {
              cqlSession.executeAsync(ops);
              latch.countDown();
            }
          }
        }

        @Override
        public void onKeyspaceDropped(@NotNull KeyspaceMetadata keyspace) {
          super.onKeyspaceDropped(keyspace);
          if (keyspace.getName().asCql(true).equals("replicator"))
            cqlSession.executeAsync(createKeyspaceOps[0]);
          if (keyspace.getName().asCql(true).equals("ks_test_cql_replicator"))
            cqlSession.executeAsync(createKeyspaceOps[1]);
        }
      };

  public static void main(String[] args) throws InterruptedException {
    try {
      Map<CqlIdentifier, KeyspaceMetadata> keyspaces = cqlSession.getMetadata().getKeyspaces();
      KeyspaceMetadata replicatorKeyspace = keyspaces.get(CqlIdentifier.fromCql("replicator"));
      KeyspaceMetadata testKeyspace =
          keyspaces.get(CqlIdentifier.fromCql("ks_test_cql_replicator"));

      if (replicatorKeyspace != null) cqlSession.execute(dropOps[0]);
      if (testKeyspace != null) cqlSession.execute(dropOps[1]);

      if (replicatorKeyspace == null) {
        cqlSession.execute(createKeyspaceOps[0]);
      }
      if (testKeyspace == null) {
        cqlSession.execute(createKeyspaceOps[1]);
      }

      latch.await();
    } finally {
      cqlSession.close();
    }
  }
}
