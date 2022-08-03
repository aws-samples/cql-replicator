// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.amazon.aws.cqlreplicator.models.StatsAggrQuery;
import com.amazon.aws.cqlreplicator.storage.IonEngine;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Responsible for providing statistic from replicator.stats */
public class Stats {

  private final CqlSession keyspacesConnector;

  public Stats(Properties config) {
    var connectionFactory = new ConnectionFactory(config);
    this.keyspacesConnector = connectionFactory.buildCqlSession("KeyspacesConnector.conf");
  }

  private String getStatsData(final String dbQuery) throws JsonProcessingException {

    var simpleStatement =
        SimpleStatement.newInstance(dbQuery)
            .setIdempotent(true)
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

    var mapper = new ObjectMapper();
    var resultSet = keyspacesConnector.execute(simpleStatement);
    var rootNode = JsonNodeFactory.instance.objectNode();
    var rs = rootNode.putArray("resultSet");
    for (Row row : resultSet) {
      var mappedValue = mapper.readTree(row.getString(0));
      rs.add(mappedValue);
    }
    return rootNode.toPrettyString();
  }

  public Map<String, JsonNode> getStatsReport(
      final String dbQuery, final Set<StatsAggrQuery> partiQLs) throws JsonProcessingException {
    var result = getStatsData(dbQuery);
    var ionEngine = new IonEngine();
    Map<String, JsonNode> jsonNodes = new HashMap<>();
    for (StatsAggrQuery statsAggrQuery : partiQLs) {
      var res = ionEngine.query(statsAggrQuery.getPartiQL(), result);
      var mapper = new ObjectMapper();
      var actualObj = mapper.readTree(res);
      jsonNodes.put(statsAggrQuery.getDescription(), actualObj);
    }
    return jsonNodes;
  }
}
