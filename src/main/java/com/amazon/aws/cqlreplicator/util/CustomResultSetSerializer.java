/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.util;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.datastax.oss.driver.api.core.type.DataType;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CustomResultSetSerializer extends StdSerializer<Row> {

    private enum JsonTypes {
        STRING,
        BINARY,
        NUMBER,
        UNKNOWN
    };

    public CustomResultSetSerializer() {
        this(null);
    }

    public CustomResultSetSerializer(Class<Row> t) {
        super(t);
    }

  void getTypedCollection(Row row, int i, JsonGenerator jsonGenerator, String name)
      throws IOException {
    Collection<?> collectionValue = null;
    var unknown = "";
    var jsonType = JsonTypes.UNKNOWN;
    if (DataTypes.setOf(DataTypes.TEXT).equals(row.getType(i))) {
      collectionValue = row.getSet(i, String.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.INT).equals(row.getType(i))) {
      collectionValue = row.getSet(i, Integer.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.setOf(DataTypes.FLOAT).equals(row.getType(i))) {
      collectionValue = row.getSet(i, Float.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.setOf(DataTypes.DOUBLE).equals(row.getType(i))) {
      collectionValue = row.getSet(i, Double.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.setOf(DataTypes.ASCII).equals(row.getType(i))) {
      collectionValue = row.getSet(i, String.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.BIGINT).equals(row.getType(i))) {
      collectionValue = row.getSet(i, Long.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.setOf(DataTypes.BLOB).equals(row.getType(i))) {
      collectionValue = row.getSet(i, ByteBuffer.class);
      jsonType = JsonTypes.BINARY;
    } else if (DataTypes.setOf(DataTypes.BOOLEAN).equals(row.getType(i))) {
      collectionValue = row.getSet(i, Boolean.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.COUNTER).equals(row.getType(i))) {
      collectionValue = row.getSet(i, Integer.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.setOf(DataTypes.DATE).equals(row.getType(i))) {
      collectionValue = row.getSet(i, LocalDate.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.DECIMAL).equals(row.getType(i))) {
      collectionValue = row.getSet(i, BigDecimal.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.SMALLINT).equals(row.getType(i))) {
      collectionValue = row.getSet(i, Integer.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.UUID).equals(row.getType(i))) {
      collectionValue = row.getSet(i, UUID.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.TIMEUUID).equals(row.getType(i))) {
      collectionValue = row.getSet(i, UUID.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.TINYINT).equals(row.getType(i))) {
      collectionValue = row.getSet(i, Byte.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.setOf(DataTypes.TIMESTAMP).equals(row.getType(i))) {
      collectionValue = row.getSet(i, Instant.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.INET).equals(row.getType(i))) {
      collectionValue = row.getSet(i, InetAddress.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.setOf(DataTypes.TEXT).equals(row.getType(i))) {
      collectionValue = row.getSet(i, String.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.INT).equals(row.getType(i))) {
      collectionValue = row.getList(i, Integer.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.listOf(DataTypes.FLOAT).equals(row.getType(i))) {
      collectionValue = row.getList(i, Float.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.listOf(DataTypes.DOUBLE).equals(row.getType(i))) {
      collectionValue = row.getList(i, Double.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.listOf(DataTypes.ASCII).equals(row.getType(i))) {
      collectionValue = row.getList(i, String.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.BIGINT).equals(row.getType(i))) {
      collectionValue = row.getList(i, Long.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.listOf(DataTypes.BLOB).equals(row.getType(i))) {
      collectionValue = row.getList(i, ByteBuffer.class);
      jsonType = JsonTypes.BINARY;
    } else if (DataTypes.listOf(DataTypes.BOOLEAN).equals(row.getType(i))) {
      collectionValue = row.getList(i, Boolean.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.COUNTER).equals(row.getType(i))) {
      collectionValue = row.getList(i, Long.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.listOf(DataTypes.DATE).equals(row.getType(i))) {
      collectionValue = row.getList(i, LocalDate.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.DECIMAL).equals(row.getType(i))) {
      collectionValue = row.getList(i, BigDecimal.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.SMALLINT).equals(row.getType(i))) {
      collectionValue = row.getList(i, Integer.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.UUID).equals(row.getType(i))) {
      collectionValue = row.getList(i, UUID.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.TIMEUUID).equals(row.getType(i))) {
      collectionValue = row.getList(i, UUID.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.TINYINT).equals(row.getType(i))) {
      collectionValue = row.getList(i, Integer.class);
      jsonType = JsonTypes.NUMBER;
    } else if (DataTypes.listOf(DataTypes.TIMESTAMP).equals(row.getType(i))) {
      collectionValue = row.getList(i, Instant.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.INET).equals(row.getType(i))) {
      collectionValue = row.getList(i, InetAddress.class);
      jsonType = JsonTypes.STRING;
    } else if (DataTypes.listOf(DataTypes.TEXT).equals(row.getType(i))) {
      collectionValue = row.getList(i, String.class);
      jsonType = JsonTypes.STRING;
      } else {
        unknown = row.get(i, String.class);
    }

      if (jsonType.equals(JsonTypes.BINARY)) {
        jsonGenerator.writeArrayFieldStart(name);
        for (Object val : Objects.requireNonNull(collectionValue)) {
            var s =  Bytes.toHexString((ByteBuffer) val);
          jsonGenerator.writeString(s);
        }
        jsonGenerator.writeEndArray();

      } else if (jsonType.equals(JsonTypes.NUMBER)) {
        jsonGenerator.writeArrayFieldStart(name);
        for (Object val : Objects.requireNonNull(collectionValue)) {
          jsonGenerator.writeNumber(String.valueOf(val));
        }
        jsonGenerator.writeEndArray();

      } else if (jsonType.equals(JsonTypes.STRING)) {
        jsonGenerator.writeArrayFieldStart(name);
        for (Object val : Objects.requireNonNull(collectionValue)) {
          jsonGenerator.writeString((String) val);
        }
        jsonGenerator.writeEndArray();
      } else {
          jsonGenerator.writeStringField(name, Objects.requireNonNull(unknown));
      }
    }

    void writeItem(Row row, int i, String name, DataType dt, JsonGenerator jgen) throws IOException {
        if (DataTypes.BOOLEAN.equals(dt)) {
            jgen.writeBooleanField(name, row.getBoolean(i));
        } else if(DataTypes.INT.equals(dt)) {
            jgen.writeNumberField(name, row.getInt(i));
        } else if(DataTypes.TEXT.equals(dt)) {
            jgen.writeStringField(name, row.getString(i));
        } else if(DataTypes.ASCII.equals(dt)) {
            jgen.writeStringField(name, row.getString(i));
        } else if(DataTypes.BIGINT.equals(dt)) {
            jgen.writeNumberField(name, row.getLong(i));
        } else if(DataTypes.BLOB.equals(dt)) {

            var s =  Bytes.toHexString(row.getByteBuffer(i).array());
            jgen.writeStringField(name, s);

        } else if(DataTypes.COUNTER.equals(dt)) {
            jgen.writeNumberField(name, row.getLong(i));
        } else if(DataTypes.DATE.equals(dt)) {
            jgen.writeStringField(name, Objects.requireNonNull(row.getLocalDate(i)).format(DateTimeFormatter.ISO_LOCAL_DATE));
        } else if(DataTypes.DECIMAL.equals(dt)) {
            jgen.writeNumberField(name, row.getBigDecimal(i));
        } else if(DataTypes.DOUBLE.equals(dt)) {
            jgen.writeNumberField(name, row.getDouble(i));
        } else if(DataTypes.FLOAT.equals(dt)) {
            jgen.writeNumberField(name, row.getFloat(i));
        } else if(DataTypes.TIMESTAMP.equals(dt)) {
            var z = row.get(i, Instant.class);
            jgen.writeStringField(name, Objects.requireNonNull(z).toString().replace("T"," "));
        } else if(DataTypes.SMALLINT.equals(dt)) {
            jgen.writeNumberField(name, row.getInt(i));
        } else if(DataTypes.TIMEUUID.equals(dt)) {
            jgen.writeStringField(name, Objects.requireNonNull(row.getUuid(i)).toString());
        } else if(DataTypes.UUID.equals(dt)) {
            jgen.writeStringField(name, Objects.requireNonNull(row.getUuid(i)).toString());
        } else if(DataTypes.TINYINT.equals(dt)) {
            jgen.writeNumberField(name, row.getByte(i));
        } else {
            getTypedCollection(row, i, jgen, name);
        }
    }

    @Override
    public void serialize(Row row, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        var cd = row.getColumnDefinitions();
        var names = new ArrayList<String>();
        var types = new ArrayList<DataType>();

        for (var column : cd) {
            names.add(column.getName().asCql(true));
            types.add(column.getType());
        }
        //for (Row row : rs) {
            jgen.writeStartObject();
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                if (row.isNull(i)) {
                    jgen.writeNullField(name);
                } else {
                    writeItem(row, i, name, types.get(i), jgen);
                }
            }
            jgen.writeEndObject();
        //}
    }

}
