/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.amazon.aws.cqlreplicator.util.Utils.putMetricData;

public class PreflightCheck implements AutoCloseable {

    public static final String TEXT_RED = "\u001B[31m";
    public static final String TEXT_GREEN = "\u001B[32m";
    public static final String TEXT_RESET = "\u001B[0m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    private static final Logger LOGGER = LoggerFactory.getLogger(PreflightCheck.class);
    private final ConnectionFactory connectionFactory;
    private final File file;
    private final String targetKeyspace;
    private final String targetTable;
    private final boolean isCloudWatch;
    private final String cloudWatchRegion;
    private CqlSession keyspacesConnector;
    private CqlSession cassandraConnector;
    private String rsSample;
    private final String source;
    public PreflightCheck(Properties config) {
        this.connectionFactory = new ConnectionFactory(config);
        this.file = new File(config.getProperty("LOCAL_STORAGE_PATH"));
        this.targetKeyspace = config.getProperty("TARGET_KEYSPACE");
        this.targetTable = config.getProperty("TARGET_TABLE");
        this.source = String.format("%s.%s", config.getProperty("SOURCE_KEYSPACE"), config.getProperty("SOURCE_TABLE"));
        this.isCloudWatch = config.getProperty("ENABLE_CLOUD_WATCH").equals("true");
        this.cloudWatchRegion = isCloudWatch ? config.getProperty("CLOUD_WATCH_REGION") : "";
    }

    @Override
    public void close() {
        keyspacesConnector.close();
        cassandraConnector.close();
    }

    private String prepareOutput(String statement, PreflightCheckStatus status) {
        var col = status.equals(PreflightCheckStatus.FAILED) ? TEXT_RED : TEXT_GREEN;
        var statusInText = status.equals(PreflightCheckStatus.FAILED) ? "FAILED" : "PASSED";
        return String.format("%s - %s%s%s", statement, col, statusInText, TEXT_RESET);
    }

    protected void runPreFlightCheck() throws PreFlightCheckException {
        Map<String, Integer> resultSet = new HashMap<>();
        LOGGER.info(ANSI_PURPLE + "Preflight check..." + TEXT_RESET);
        var ctc = checkTargetConnectivity();
        resultSet.put(ctc.name(), 1);
        LOGGER.info(prepareOutput("Checking Amazon Keyspaces connectivity", ctc));
        var csc = checkSourceConnectivity();
        resultSet.put(csc.name(), 1);
        LOGGER.info(prepareOutput("Checking the Cassandra connectivity", csc));
        var clsta = checkLocalStorageAvailability();
        resultSet.put(clsta.name(), 1);
        LOGGER.info(prepareOutput("Checking the local storage availability", clsta));
        var cta = checkTargetAvailability();
        resultSet.put(cta.name(), 1);
        LOGGER.info(prepareOutput("Checking the target table", cta));
        var crpfs = checkReadPermissionFromSource();
        resultSet.put(crpfs.name(), 1);
        LOGGER.info(prepareOutput("Checking read permission from the source table", crpfs));
        var cwptt = checkWritePermissionToTarget();
        resultSet.put(cwptt.name(), 1);
        LOGGER.info(prepareOutput("Checking write permission to the target table", cwptt));
        if (isCloudWatch) {
            var ccwa = checkCloudWatchAvailability();
            resultSet.put(ccwa.name(), 1);
            LOGGER.info(prepareOutput("Checking the CloudWatch availability", ccwa));
        } else {
            var cssa = checkStatsAvailability();
            resultSet.put(cssa.name(), 1);
            LOGGER.info(prepareOutput("Checking the Replicator.stats availability", cssa));
        }

        if (resultSet.get("FAILED") != null) {
            System.err.println("The preflight check has failed");
            throw new PreFlightCheckException("Preflight check failed");
        }
    }

    private PreflightCheckStatus checkTargetConnectivity() {
        this.keyspacesConnector = connectionFactory.buildCqlSession("KeyspacesConnector.conf");
        LOGGER.debug(String.valueOf(this.keyspacesConnector.getMetadata().getNodes()));
        return (keyspacesConnector.isClosed()) ? PreflightCheckStatus.FAILED : PreflightCheckStatus.PASSED;
    }

    private PreflightCheckStatus checkSourceConnectivity() {
        this.cassandraConnector = connectionFactory.buildCqlSession("CassandraConnector.conf");
        LOGGER.debug(String.valueOf(this.cassandraConnector.getMetadata().getNodes()));
        return (cassandraConnector.isClosed()) ? PreflightCheckStatus.FAILED : PreflightCheckStatus.PASSED;
    }

    private PreflightCheckStatus checkLocalStorageAvailability() {
        return (file.canWrite() && file.canRead()) ? PreflightCheckStatus.PASSED : PreflightCheckStatus.FAILED;
    }

    private PreflightCheckStatus checkTargetAvailability() {
        return (this.keyspacesConnector.getMetadata().getKeyspace(targetKeyspace).flatMap(keyspace -> keyspace.getTable(targetTable)).isPresent()) ?
                PreflightCheckStatus.PASSED : PreflightCheckStatus.FAILED;
    }

    private PreflightCheckStatus checkCloudWatchAvailability() {
        try {
            var cloudWatchClient =
                    CloudWatchClient.builder()
                            .region(Region.of(cloudWatchRegion))
                            .build();
            putMetricData(cloudWatchClient, 1.0, "Pre-flight-check");
            cloudWatchClient.close();
        } catch (CloudWatchException | SdkClientException e) {
            return PreflightCheckStatus.FAILED;
        }
        return PreflightCheckStatus.PASSED;
    }

    private PreflightCheckStatus checkStatsAvailability() {
        return (this.keyspacesConnector.getMetadata().getKeyspace("replicator").flatMap(replicator -> replicator.getTable("stats")).isPresent()) ?
                PreflightCheckStatus.PASSED : PreflightCheckStatus.FAILED;
    }

    private PreflightCheckStatus checkWritePermissionToTarget() {
        if (!rsSample.isEmpty()) {
            try {
                var rs = keyspacesConnector.execute(String.format("INSERT INTO %s.%s JSON '%s'", targetKeyspace, targetTable, rsSample)).wasApplied();
            } catch (WriteTimeoutException | WriteFailureException | QueryValidationException |
                     UnavailableException e) {
                return PreflightCheckStatus.FAILED;
            }
        }
        return PreflightCheckStatus.PASSED;
    }

    private PreflightCheckStatus checkReadPermissionFromSource() {
        var sourceSampleQuery = String.format("SELECT json * FROM %s LIMIT 1", source);
        try {
            var rs = cassandraConnector.execute(sourceSampleQuery).one();
            if (rs != null) {
                this.rsSample = rs.getString(0);
            } else {
                this.rsSample = "";
            }
        } catch (ReadTimeoutException | ReadFailureException | QueryValidationException | UnavailableException e) {
            return PreflightCheckStatus.FAILED;
        }
        return PreflightCheckStatus.PASSED;
    }

    private enum PreflightCheckStatus {
        PASSED,
        FAILED
    }

}
