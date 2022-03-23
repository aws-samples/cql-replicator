// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.truncate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Responsible for testing base functionality of CQLReplicator
 * Before run the test set env variable CQLREPLICATOR_CONF
 *
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CqlReplicatorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CqlReplicatorTest.class);
    private static final String keyspaceName = "ks_test_cql_replicator";
    private static final String tableName = "test_cql_replicator";
    private static final String pathToConfig = System.getenv("CQLREPLICATOR_CONF");
    private static final File configFileK =
            new File(String.format("%s/%s", pathToConfig, "KeyspacesConnector.conf"));
    private static final File configFileC =
            new File(String.format("%s/%s", pathToConfig, "CassandraConnector.conf"));

    private static long sourceCount = 0;
    private static long targetCount = 0;
    private static Set<String> sourceHash = new HashSet<>();
    private static Set<String> targetHash = new HashSet<>();
    private static CqlSession keyspacesConnectorSession = CqlSession.builder()
            .withConfigLoader(DriverConfigLoader.fromFile(configFileK))
            .addTypeCodecs(TypeCodecs.ZONED_TIMESTAMP_UTC)
            .build();
    private static CqlSession cassandraConnectorSession = CqlSession.builder()
            .withConfigLoader(DriverConfigLoader.fromFile(configFileC))
            .addTypeCodecs(TypeCodecs.ZONED_TIMESTAMP_UTC)
            .build();

    /**
     * Setting up Cassandra and Amazon Keyspaces sessions
     *
     */

    @BeforeAll
    static void setup() {

        Select query = selectFrom(keyspaceName, tableName).json().all();
        SimpleStatement statement = query.build();

        ResultSet rsTarget = keyspacesConnectorSession.execute(statement);
        ResultSet rsSource = cassandraConnectorSession.execute(statement);

        rsSource.all().forEach(row -> {
            sourceHash.add(DigestUtils.md5Hex(Objects.requireNonNull(row.getString(0)).
                    replace("'", "\\\\u0027")));
            sourceCount++;
        });

        rsTarget.all().forEach(row -> {
            targetHash.add(DigestUtils.md5Hex(row.getString(0)));
            targetCount++;
        });

        LOGGER.info("@BeforeAll - executes once before all test methods in this class");
    }

    /**
     * Testing correctness of target dataset by counting source and target
     */

    @Test
    @Order(1)
    void countAssumption() {
        assertEquals(sourceCount, targetCount);
        LOGGER.info("The number of rows in the source table {}", sourceCount);
        LOGGER.info("The number of rows in the target table {}", targetCount);
    }

    /**
     * Testing correctness of target dataset by comparing hashes in source and target
     */

    @Test
    @Order(2)
    void dataQualityAssumption() {
        try {
            assertEquals(sourceHash, targetHash);
            System.out.println("dataQualityAssumption: source dataset is equal to target dataset");

        } catch (AssertionError e) {
            System.out.println("dataQualityAssumption: source dataset is not equal to target dataset");
            fail(e);
        }
    }

    /**
     * Testing correctness of dataset after updating all rows in source dataset
     *
     */

    @Test
    @Order(3)
    void updateAssumption() {
        Select query = selectFrom(keyspaceName, tableName).columns("key", "col0");
        SimpleStatement statement = query.build();
        ResultSet rsSource = cassandraConnectorSession.execute(statement);
        rsSource.all().forEach(row -> {

            LocalDate localDate = LocalDate.now();
            UUID uuid = row.getUuid("key");
            byte col0 = row.getByte("col0");

            PreparedStatement updatePreparedStatement = cassandraConnectorSession.prepare(String.format("UPDATE %s.%s SET COL2=:COL2 WHERE KEY=:KEY AND COL0=:COL0 ", keyspaceName, tableName));
            BoundStatementBuilder boundStatementBuilder = updatePreparedStatement.boundStatementBuilder()
                    .setUuid("key", uuid)
                    .setByte("col0", col0)
                    .setLocalDate("col2", localDate)
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            cassandraConnectorSession.execute(boundStatementBuilder.build());

        });
        dataQualityAssumption();
    }

    /**
     * Testing correctness of target dataset after deletion in source table
     *
     * @throws InterruptedException
     */

    @Test
    @Order(4)
    void deleteAssumption() throws InterruptedException{

        Select query = selectFrom(keyspaceName, tableName).columns("key", "col0");
        SimpleStatement statement = query.build();
        ResultSet rsTarget = keyspacesConnectorSession.execute(statement);
        AtomicLong trgCnt = new AtomicLong();
        trgCnt.set(targetCount);
        rsTarget.all().forEach(row -> trgCnt.getAndDecrement());

        Truncate truncate = truncate(keyspaceName, tableName);
        SimpleStatement truncateStatement = truncate.build();
        cassandraConnectorSession.execute(truncateStatement);
        // CQLReplicator should be ready to start the replication process
        Thread.sleep(30000);
        assertEquals(0, trgCnt.get());
    }

}