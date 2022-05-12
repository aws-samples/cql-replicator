// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.extractor;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.math.BigInteger;
import java.util.*;

/** Responsible for providing extracting logic from source cluster */
public class CassandraExtractor implements DataExtractor {
  private static final SimpleStatement statement =
      SimpleStatement.newInstance(
          "select column_name, type, position, kind from system_schema.\"columns\" "
              + "where keyspace_name=:keyspace_name and table_name=:table_name");
  private final Map<String, LinkedHashMap<String, String>> metaData;
  private final CqlSession cassandraSession;
  private final Properties config;
  private final PreparedStatement psCassandra;

  private final String BIG_INT_MAX_VALUE = String.valueOf(2^Integer.MAX_VALUE);
  private final String BIG_INT_MIN_VALUE = String.valueOf(-2^Integer.MIN_VALUE);

  public CassandraExtractor(Properties config) {
    this.config = config;
    ConnectionFactory connectionFactory = new ConnectionFactory(config);
    this.cassandraSession = connectionFactory.buildCqlSession("CassandraConnector.conf");
    this.psCassandra = cassandraSession.prepare(config.getProperty("SOURCE_CQL_QUERY"));
    metaData =
        getColumns(config.getProperty("TARGET_KEYSPACE"), config.getProperty("TARGET_TABLE"));
  }

  public List<Row> findPartitionsByTokenRange(String pksStr, long startRange, long endRange) {

    BoundStatementBuilder psPksbyRange = null;

    if (startRange < endRange) {
      psPksbyRange =
          getPartitionKeysByTokenRange(pksStr, startRange, endRange)
              .boundStatementBuilder()
              .setLong("r1", startRange)
              .setLong("r2", endRange);
    } else if (startRange > endRange) {
      psPksbyRange =
          getPartitionKeysByTokenRange(pksStr, startRange, endRange)
              .boundStatementBuilder()
              .setLong("r1", startRange);
    }
    BoundStatement boundStatement = psPksbyRange.build();
    return cassandraSession
        .execute(boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
        .all();
  }

  public List<ImmutablePair<String, String>> getTokenRanges() {
    List<ImmutablePair<String, String>> ranges = new ArrayList<>();
    Metadata metadata = cassandraSession.getMetadata();
    TokenMap tokenMap = metadata.getTokenMap().get();

    for (TokenRange range : tokenMap.getTokenRanges()) {
      if (range.getStart() instanceof Murmur3Token) {
        long start = ((Murmur3Token) range.getStart()).getValue();
        long end = ((Murmur3Token) range.getEnd()).getValue();
        if (start < end) {
          ranges.add(new ImmutablePair<>(String.valueOf(start), String.valueOf(end)));
        } else if (start > end) {
          ranges.add(new ImmutablePair<>(String.valueOf(start), String.valueOf(Long.MAX_VALUE)));
          ranges.add(new ImmutablePair<>(String.valueOf(Long.MIN_VALUE), String.valueOf(end)));
        }
      }
      //To support Cassandra<2.1 clusters
      if (range.getStart() instanceof RandomToken) {
        BigInteger start = ((RandomToken) range.getStart()).getValue();
        BigInteger end = ((RandomToken) range.getEnd()).getValue();
        if (start.compareTo(end) == -1) {
          ranges.add(new ImmutablePair<>(String.valueOf(start), String.valueOf(end)));
        } else if (start.compareTo(end) == 1) {
          ranges.add(new ImmutablePair<>(String.valueOf(start), BIG_INT_MAX_VALUE));
          ranges.add(new ImmutablePair<>(BIG_INT_MIN_VALUE, String.valueOf(end)));
        }
      }
      }
    return ranges;
  }


  @Override
  public List<Row> extract(Object object) {
    ResultSet resultSet = cassandraSession.execute(((BoundStatementBuilder) object).build());
    return resultSet.all();
  }

  private PreparedStatement getPartitionKeysByTokenRange(
      String partitionKeyStr, long startRange, long endRange) {

    String finalCqlStatement =
        String.format(
            "select distinct %s from %s.%s where token(%s)>=:r1 and token(%s)<=:r2",
            partitionKeyStr,
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"),
            partitionKeyStr,
            partitionKeyStr);

    if (startRange < endRange) {
      finalCqlStatement =
          String.format(
              "select distinct %s from %s.%s where token(%s)>=:r1 and token(%s)<=:r2",
              partitionKeyStr,
              config.getProperty("TARGET_KEYSPACE"),
              config.getProperty("TARGET_TABLE"),
              partitionKeyStr,
              partitionKeyStr);
    } else if (endRange < startRange) {
      finalCqlStatement =
          String.format(
              "select distinct %s from %s.%s where token(%s)>=:r1",
              partitionKeyStr,
              config.getProperty("TARGET_KEYSPACE"),
              config.getProperty("TARGET_TABLE"),
              partitionKeyStr,
              partitionKeyStr);
    }

    return cassandraSession.prepare(finalCqlStatement);
  }

  public PreparedStatement getCassandraPreparedStatement() {
    return psCassandra;
  }

  public Map<String, LinkedHashMap<String, String>> getColumns(
      String keyspaceName, String tableName) {
    Map<Integer, Map<String, String>> partitionKeysTemp = new LinkedHashMap<>();
    LinkedHashMap<String, String> partitionKeys = new LinkedHashMap<>();
    LinkedHashMap<String, String> clusteringKeys = new LinkedHashMap<>();
    LinkedHashMap<String, String> regularColumns = new LinkedHashMap<>();
    Map<String, LinkedHashMap<String, String>> finalMetaData = new HashMap<>();

    ResultSet resultSet =
        cassandraSession.execute(
            SimpleStatement.builder(statement)
                .addNamedValue("keyspace_name", keyspaceName)
                .addNamedValue("table_name", tableName)
                .build());

    for (Row row : resultSet) {
      if (Objects.requireNonNull(row.getString("kind")).equals("partition_key")) {
        partitionKeysTemp.put(
            row.getInt("position"),
            Collections.singletonMap(row.getString("column_name"), row.getString("type")));
      }
      if (Objects.requireNonNull(row.getString("kind")).equals("clustering")) {
        clusteringKeys.put(row.getString("column_name"), row.getString("type"));
      }
      if (Objects.requireNonNull(row.getString("kind")).equals("regular")
          || Objects.requireNonNull(row.getString("kind")).equals("static")) {
        regularColumns.put(row.getString("column_name"), row.getString("type"));
      }
    }

    partitionKeysTemp.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              String newKey = (String) entry.getValue().keySet().toArray()[0];
              String newValue = (String) entry.getValue().values().toArray()[0];
              partitionKeys.put(newKey, newValue);
            });

    finalMetaData.put("partition_key", partitionKeys);
    finalMetaData.put("clustering", clusteringKeys);
    finalMetaData.put("regular", regularColumns);

    return finalMetaData;
  }

  public Map<String, LinkedHashMap<String, String>> getMetaData() {
    return metaData;
  }
}
