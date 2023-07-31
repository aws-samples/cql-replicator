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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
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
            //TODO: Add support without writime columns
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
            case "blob" -> bound.setByteBuffer(columnName, ByteBuffer.wrap(colValue.getBytes()));
            case "bigint" -> bound.setLong(columnName, Long.parseLong(colValue));
            case "tinyint" -> bound.setByte(columnName, Byte.parseByte(colValue));
            case "smallint" -> bound.setShort(columnName, Short.parseShort(colValue));
            case "int", "counter" -> bound.setInt(columnName, Integer.parseInt(colValue));
            case "ascii", "text", "varchar" -> bound.setString(columnName, colValue);
            case "double" -> bound.setDouble(columnName, Double.parseDouble(colValue));
            case "float" -> bound.setFloat(columnName, Float.parseFloat(colValue));
            case "date" -> {
                var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                var date = java.time.LocalDate.parse(colValue, formatter);
                bound.setLocalDate(columnName, date);
            }
            case "timestamp" -> {
                var valueOnClient = ZonedDateTime.parse(colValue.replace("\"", ""));
                bound.set(columnName, valueOnClient, GenericType.ZONED_DATE_TIME);
            }
            case "timeuuid", "uuid" -> bound.setUuid(columnName, UUID.fromString(colValue));
            case "decimal" -> bound.setBigDecimal(columnName, new BigDecimal(colValue));
            case "varint" -> bound.setBigInteger(columnName, new BigInteger(colValue));
            case "boolean" -> bound.setBoolean(columnName, Boolean.parseBoolean(colValue));
            case "inet" -> {
                try {
                    var ipFixed = InetAddress.getByName(colValue.replace("/", ""));
                    bound.setInetAddress(columnName, ipFixed);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> {
                LOGGER.warn("Unrecognized data type {}", cqlType);
                bound.setString(columnName, colValue);
            }
        }
        return bound;
    }

    public static GenericType getClassType(String cqlType) {
        GenericType genericType;
        switch (cqlType) {
            case "BIGINT" -> genericType = GenericType.LONG;
            case "SMALLINT" -> genericType = GenericType.SHORT;
            case "TINYINT" -> genericType = GenericType.BYTE;
            case "INT", "COUNTER" -> genericType = GenericType.INTEGER;
            case "ASCII", "TEXT", "VARCHAR" -> genericType = GenericType.STRING;
            case "UUID", "TIMEUUID" -> genericType = GenericType.UUID;
            case "DOUBLE" -> genericType = GenericType.DOUBLE;
            case "FLOAT" -> genericType = GenericType.FLOAT;
            case "DATE" -> genericType = GenericType.LOCAL_DATE;
            case "DATETIME" -> genericType = GenericType.LOCAL_DATE_TIME;
            case "DECIMAL" -> genericType = GenericType.BIG_DECIMAL;
            case "VARINT" -> genericType = GenericType.BIG_INTEGER;
            case "INET" -> genericType = GenericType.INET_ADDRESS;
            case "BOOLEAN" -> genericType = GenericType.BOOLEAN;
            case "BLOB" -> genericType = GenericType.BYTE_BUFFER;
            case "LIST<TEXT>" -> genericType = GenericType.listOf(String.class);
            case "LIST<FLOAT>" -> genericType = GenericType.listOf(Float.class);
            case "LIST<INT>" -> genericType = GenericType.listOf(Integer.class);
            case "LIST<DATE>" -> genericType = GenericType.listOf(java.time.LocalDate.class);
            case "LIST<DOUBLE>" -> genericType = GenericType.listOf(Double.class);
            case "LIST<DECIMAL>" -> genericType = GenericType.listOf(BigDecimal.class);
            case "LIST<BIGINT>" -> genericType = GenericType.listOf(Long.class);
            case "SET<TEXT>" -> genericType = GenericType.setOf(String.class);
            case "SET<DATE>" -> genericType = GenericType.setOf(java.time.LocalDate.class);
            case "SET<INT>" -> genericType = GenericType.setOf(Integer.class);
            case "SET<DOUBLE>" -> genericType = GenericType.setOf(Double.class);
            case "SET<DECIMAL>" -> genericType = GenericType.setOf(BigDecimal.class);
            case "SET<BIGINT>" -> genericType = GenericType.setOf(Long.class);
            default -> {
                LOGGER.warn("Unrecognized data type:{}", cqlType);
                genericType = GenericType.STRING;
            }
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
        SYNC_DELETED_ROWS,
        SYNC_DELETED_PARTITION_KEYS
    }
}
