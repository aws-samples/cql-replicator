// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.util;

import com.amazon.aws.cqlreplicator.models.Payload;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utils {

    private static final Pattern REGEX_COM = Pattern.compile(",");

    private static final Pattern REGEX_REG_SPACE = Pattern.compile(" ");
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
    private static final Pattern REGEX_DQ = Pattern.compile("\"[^\"]*\"");

    public static String getSourceQuery(Properties properties, String[] partitionKeys) {

        var sourceQueryBuilder = new StringBuilder();
        var sourceKeyspace = properties.getProperty("SOURCE_KEYSPACE");
        var sourceTable = properties.getProperty("SOURCE_TABLE");
        var columns = properties.getProperty("REPLICATED_COLUMNS");
        var json = properties.getProperty("CASSANDRA_JSON_SERIALIZER").equals("true") ? "json" : "";
        var writeTimeColumns = properties.getProperty("WRITETIME_COLUMNS").split(",");
        var writeTimeStatements = new ArrayList<String>();
        var partitionKeysPredicates = new ArrayList<String>();
        Arrays.stream(partitionKeys).forEach(col -> partitionKeysPredicates.add(String.format("%s=:%s", col, col)));
        Arrays.stream(writeTimeColumns).forEach(col -> writeTimeStatements.add(String.format("writetime(%s)", col)));

        sourceQueryBuilder.append("SELECT ").
                append(json).
                append(" ").
                append(columns).
                append(",").
                append(String.join(",", writeTimeStatements)).
                append(" FROM ").
                append(sourceKeyspace).
                append(".").
                append(sourceTable).
                append(" WHERE ").
                append(String.join(" AND ", partitionKeysPredicates));

        return sourceQueryBuilder.toString();

    }

    public static int bytesToInt(byte[] value) {
        return ByteBuffer.wrap(value).getInt();
    }

    public static long bytesToLong(byte[] value) {
        return ByteBuffer.wrap(value).getLong();
    }

    public static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();

    }

    public static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private static String toHexString(byte[] data) {
        var rs = new Formatter();
        try (rs) {
            for (var b : data) {
                rs.format("%02x", b & 0xff);
            }
            return rs.toString();
        }
    }

    public static void putMetricData(CloudWatchClient cw, Double dataPoint, String metricName) {
        try {
            Dimension dimension = Dimension.builder()
                    .name("OPERATIONS")
                    .value("REPLICA")
                    .build();

            // Set an Instant object
            String time = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
            Instant instant = Instant.parse(time);

            MetricDatum datum = MetricDatum.builder()
                    .metricName(metricName)
                    .unit(StandardUnit.COUNT)
                    .value(dataPoint)
                    .timestamp(instant)
                    .dimensions(dimension).build();

            PutMetricDataRequest request = PutMetricDataRequest.builder()
                    .namespace("CQL-REPLICATOR")
                    .metricData(datum).build();

            cw.putMetricData(request);
        } catch (CloudWatchException e) {
            throw new RuntimeException(e);
        }
    }

    public static String doubleQuoteResolver(String source, String input) {
        var matcher = REGEX_DQ.matcher(input);
        var action = matcher.find();
        if (action) {
            return source.replace("%s.%s", "\"%s\".\"%s\"");
        } else
            return source;
    }

    public static byte[] compress(byte[] payload) throws IOException {
        return Snappy.compress(payload);
    }

    public static byte[] decompress(byte[] payload) throws IOException {
        return Snappy.uncompress(payload);
    }

    public static <T> byte[] cborEncoder(List<T> obj) throws JsonProcessingException {
        var mapper = new CBORMapper();
        return mapper.writeValueAsBytes(obj);
    }

    public static <T> List<T> cborDecoder(byte[] payload) throws IOException {
        var mapper = new CBORMapper();
        return mapper.readValue(payload, List.class);
    }

    public static <T> byte[] cborEncoderSet(Set<T> obj) throws JsonProcessingException {
        var mapper = new CBORMapper();
        return mapper.writeValueAsBytes(obj);
    }

    public static <T> Set<T> cborDecoderSet(byte[] payload) throws IOException {
        var mapper = new CBORMapper();
        return mapper.readValue(payload, Set.class);
    }

    public static <T> Stream<T> getSliceOfStream(Stream<T> stream, int startIndex, int endIndex) {
        return stream.skip(startIndex).limit(endIndex - startIndex + 1);
    }

    public static List<List<ImmutablePair<String, String>>> getDistributedRangesByTiles(
            List<ImmutablePair<String, String>> ranges, int tiles) {

        LinkedList<List<ImmutablePair<String, String>>> partitionedTokenRanges = new LinkedList<>();
        var totalRanges = ranges.size();

        FlushingList<ImmutablePair<String, String>> flushingList =
                new FlushingList<>(Math.floorDiv(totalRanges, tiles)) {
                    @Override
                    protected void flush(List payload) {
                        partitionedTokenRanges.add(payload);
                    }
                };

        ranges.forEach(flushingList::put);

        if (flushingList.getSize() > 0) {
            flushingList.doFlush();
        }

        // Let's do merge
        if (partitionedTokenRanges.size() > tiles) {

            Stream<List<ImmutablePair<String, String>>> sliceOfListStream =
                    getSliceOfStream(
                            partitionedTokenRanges.stream(), tiles - 1, partitionedTokenRanges.size());

            int rangesToRemove = partitionedTokenRanges.size() - tiles;

            List<ImmutablePair<String, String>> merged =
                    sliceOfListStream.flatMap(Collection::parallelStream).collect(Collectors.toList());

            for (int i = 0; i <= rangesToRemove; i++) {
                partitionedTokenRanges.removeLast();
            }

            partitionedTokenRanges.add(merged);
        }

        return partitionedTokenRanges;
    }

    public static Payload convertToJson(
            String rawData, String writeTimeColumns, String[] cls, String[] pks) {
        var objectMapper = new ObjectMapper();

        var payload = new Payload();

        Map<String, String> clusteringColumnMapping = new HashMap<>();
        Map<String, String> partitionColumnsMapping = new HashMap<>();

        var writeTimeColumnsArray =
                REGEX_COM.split(REGEX_REG_SPACE.matcher(writeTimeColumns).replaceAll(""));

        List<Long> writeTimeArray = new ArrayList<>();

        JsonNode rootNode;
        try {
            rootNode = objectMapper.readTree(rawData);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        for (String writeColumn : writeTimeColumnsArray) {
            JsonNode idNode =
                    Objects.requireNonNull(rootNode).path(String.format("writetime(%s)", writeColumn));
            writeTimeArray.add(idNode.asLong());
        }
        for (String cln : cls) {
            clusteringColumnMapping.put(cln, Objects.requireNonNull(rootNode).path(cln).asText());
        }

        for (String pk : pks) {
            partitionColumnsMapping.put(pk, Objects.requireNonNull(rootNode).path(pk).asText());
        }

        payload.setPk(String.join("|", partitionColumnsMapping.values()));
        payload.setClusteringColumn(String.join("|", clusteringColumnMapping.values()));
        payload.setTimestamp(writeTimeArray.stream().max(Long::compare).get());
        payload.setClusteringColumns(clusteringColumnMapping);

        for (String writeColumn : writeTimeColumnsArray) {
            ((ObjectNode) rootNode).remove(String.format("writetime(%s)", writeColumn));
        }
        // Single quote invalidates INSERT JSON statements
        payload.setPayload(String.valueOf(rootNode).replace("'", "\\\\u0027"));

        return payload;
    }

    public static BoundStatementBuilder aggregateBuilder(
            String cqlType, String columnName, String colValue, BoundStatementBuilder bound) {

        switch (cqlType) {
            case "blob":
                bound.setByteBuffer(columnName, ByteBuffer.wrap(colValue.getBytes()));
                break;
            case "bigint":
                bound.setLong(columnName, Long.parseLong(colValue));
                break;
            case "tinyint":
                bound.setByte(columnName, Byte.parseByte(colValue));
                break;
            case "smallint":
                bound.setShort(columnName, Short.parseShort(colValue));
                break;
            case "int":
            case "counter":
                bound.setInt(columnName, Integer.parseInt(colValue));
                break;
            case "ascii":
            case "text":
            case "varchar":
                bound.setString(columnName, colValue);
                break;
            case "double":
                bound.setDouble(columnName, Double.parseDouble(colValue));
                break;
            case "float":
                bound.setFloat(columnName, Float.parseFloat(colValue));
                break;
            case "date":
                var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                var date = java.time.LocalDate.parse(colValue, formatter);
                bound.setLocalDate(columnName, date);
                break;
            case "timestamp":
                var valueOnClient = ZonedDateTime.parse(colValue.replace("\"", ""));
                bound.set(columnName, valueOnClient, GenericType.ZONED_DATE_TIME);
                break;
            case "timeuuid":
            case "uuid":
                bound.setUuid(columnName, UUID.fromString(colValue));
                break;
            case "decimal":
                bound.setBigDecimal(columnName, new BigDecimal(colValue));
                break;
            case "varint":
                bound.setBigInteger(columnName, new BigInteger(colValue));
                break;
            case "boolean":
                bound.setBoolean(columnName, Boolean.parseBoolean(colValue));
                break;
            case "inet":
                try {
                    var ipFixed = InetAddress.getByName(colValue.replace("/", ""));
                    bound.setInetAddress(columnName, ipFixed);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                break;
            default:
                LOGGER.warn("Unrecognized data type {}", cqlType);
                bound.setString(columnName, colValue);
                break;
        }
        return bound;
    }

    public static GenericType getClassType(String cqlType) {
        GenericType genericType;
        switch (cqlType) {
            case "BIGINT":
                genericType = GenericType.LONG;
                break;
            case "SMALLINT":
                genericType = GenericType.SHORT;
                break;
            case "TINYINT":
                genericType = GenericType.BYTE;
                break;
            case "INT":
            case "COUNTER":
                genericType = GenericType.INTEGER;
                break;
            case "ASCII":
            case "TEXT":
            case "VARCHAR":
                genericType = GenericType.STRING;
                break;
            case "UUID":
            case "TIMEUUID":
                genericType = GenericType.UUID;
                break;
            case "DOUBLE":
                genericType = GenericType.DOUBLE;
                break;
            case "FLOAT":
                genericType = GenericType.FLOAT;
                break;
            case "DATE":
                genericType = GenericType.LOCAL_DATE;
                break;
            case "DATETIME":
                genericType = GenericType.LOCAL_DATE_TIME;
                break;
            case "DECIMAL":
                genericType = GenericType.BIG_DECIMAL;
                break;
            case "VARINT":
                genericType = GenericType.BIG_INTEGER;
                break;
            case "INET":
                genericType = GenericType.INET_ADDRESS;
                break;
            case "BOOLEAN":
                genericType = GenericType.BOOLEAN;
                break;
            case "BLOB":
                genericType = GenericType.BYTE_BUFFER;
                break;
            case "LIST<TEXT>":
                genericType = GenericType.listOf(String.class);
                break;
            case "LIST<FLOAT>":
                genericType = GenericType.listOf(Float.class);
                break;
            case "LIST<INT>":
                genericType = GenericType.listOf(Integer.class);
                break;
            case "LIST<DATE>":
                genericType = GenericType.listOf(java.time.LocalDate.class);
                break;
            case "LIST<DOUBLE>":
                genericType = GenericType.listOf(Double.class);
                break;
            case "LIST<DECIMAL>":
                genericType = GenericType.listOf(BigDecimal.class);
                break;
            case "LIST<BIGINT>":
                genericType = GenericType.listOf(Long.class);
                break;
            case "SET<TEXT>":
                genericType = GenericType.setOf(String.class);
                break;
            case "SET<DATE>":
                genericType = GenericType.setOf(java.time.LocalDate.class);
                break;
            case "SET<INT>":
                genericType = GenericType.setOf(Integer.class);
                break;
            case "SET<DOUBLE>":
                genericType = GenericType.setOf(Double.class);
                break;
            case "SET<DECIMAL>":
                genericType = GenericType.setOf(BigDecimal.class);
                break;
            case "SET<BIGINT>":
                genericType = GenericType.setOf(Long.class);
                break;
            default:
                LOGGER.warn("Unrecognized data type:{}", cqlType);
                genericType = GenericType.STRING;
                break;
        }
        return genericType;
    }

    public enum HashingFunctions {
        MURMUR_HASH3_128_X64,
        SHA_256
    }

    public enum CassandraTaskTypes {
        SYNC_PARTITION_KEYS,
        SYNC_CASSANDRA_ROWS,
        SYNC_DELETED_PARTITION_KEYS
    }
}
