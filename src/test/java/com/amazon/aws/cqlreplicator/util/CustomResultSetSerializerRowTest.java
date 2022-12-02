/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.util;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CustomResultSetSerializerRowTest {
    private CqlSession session;
    private CustomResultSetSerializer customResultSetSerializerUnderTest;

@BeforeEach
void setUp() throws IOException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
    session = EmbeddedCassandraServerHelper.getSession();
    new CQLDataLoader(session).load(new ClassPathCQLDataSet("source.cql", "test_table"));
    customResultSetSerializerUnderTest = new CustomResultSetSerializer(Row.class);
}

@Test
    void testWriteItem() throws Exception {
        ResultSet expected = session.execute("select col3, writetime(col4) from test_table WHERE col1='1234' and col2=20220701");
        ResultSet actual = session.execute("select json col3, writetime(col4) from test_table WHERE col1='1234' and col2=20220701");

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Row.class, new CustomResultSetSerializer());
        mapper.registerModule(module);
        var customJson = mapper.writeValueAsString(expected.one()).replace("\\\"", "");
        var originalJson = actual.one().getString(0).replace(" ","");
        assertEquals(originalJson, customJson);

    }

}
