// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.amazon.aws.cqlreplicator.models.StatsAggrQuery;
import com.amazon.aws.cqlreplicator.storage.IonEngine;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Responsible for providing statistic from replicator.stats */
public class Stats {

  private final CqlSession keyspacesConnector;

  public Stats(Properties config) {
    ConnectionFactory connectionFactory = new ConnectionFactory(config);
    this.keyspacesConnector = connectionFactory.buildCqlSession("KeyspacesConnector.conf");
  }

  private String getStatsData(final String dbQuery) throws JsonProcessingException {

    SimpleStatement simpleStatement =
        SimpleStatement.newInstance(dbQuery)
            .setIdempotent(true)
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

    ObjectMapper mapper = new ObjectMapper();
    ResultSet resultSet = keyspacesConnector.execute(simpleStatement);
    ObjectNode rootNode = JsonNodeFactory.instance.objectNode();
    ArrayNode rs = rootNode.putArray("resultSet");
    for (Row row : resultSet) {
      JsonNode mappedValue = mapper.readTree(row.getString(0));
      rs.add(mappedValue);
    }
    return rootNode.toPrettyString();
  }

  public Map<String, JsonNode> getStatsReport(
      final String dbQuery, final Set<StatsAggrQuery> partiQLs) throws JsonProcessingException {
    String result = getStatsData(dbQuery);
    IonEngine ionEngine = new IonEngine();
    Map<String, JsonNode> jsonNodes = new HashMap<>();
    for (StatsAggrQuery statsAggrQuery : partiQLs) {
      String res = ionEngine.query(statsAggrQuery.getPartiQL(), result);
      ObjectMapper mapper = new ObjectMapper();
      JsonNode actualObj = mapper.readTree(res);
      jsonNodes.put(statsAggrQuery.getDescription(), actualObj);
    }
    return jsonNodes;
  }
}
