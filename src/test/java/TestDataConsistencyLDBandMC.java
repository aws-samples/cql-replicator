/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

import com.amazon.aws.cqlreplicator.models.PartitionKey;
import com.amazon.aws.cqlreplicator.models.TimestampMetrics;
import com.amazon.aws.cqlreplicator.util.Utils;
import org.apache.commons.lang3.SerializationUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestDataConsistencyLDBandMC {

  private static DB db1;
  private static DB db2;
  private static Map<String, Set<String>> mapOfAdjListPrimaryKeys = new HashMap<>();
  private static Map<Object, TimestampMetrics> mapOfPrimaryKeysWithTimestampMetrics = new HashMap();

  @BeforeAll
  static void setup() throws IOException {

    Options options = new Options();

    db1 =
        factory.open(
            new File("/Users/kolesnn/Work/CQLReplicator/ledger_v4_0_rd.ldb"),
            options.createIfMissing(false));
    db2 =
        factory.open(
            new File("/Users/kolesnn/Work/CQLReplicator/ledger_v4_1_rd.ldb"),
            options.createIfMissing(false));
  }


  @Test
  @Order(1)
  void populateDataFromLevelDB() throws IOException {

    var iterator1 = db1.iterator();
    var iterator2 = db2.iterator();

    try {
      for(iterator1.seekToFirst(); iterator1.hasNext(); iterator1.next()) {
        var key = SerializationUtils.deserialize(iterator1.peekNext().getKey());
        var value = iterator1.peekNext().getValue();
        if (key instanceof PartitionKey)
        {
          mapOfAdjListPrimaryKeys.put( ((PartitionKey) key).getPartitionKey(), Utils.cborDecoderSet(value));
        }
        else
        {
          mapOfPrimaryKeysWithTimestampMetrics.put(key, SerializationUtils.deserialize(value));
        }

      }
    } finally {
      // Make sure you close the iterator to avoid resource leaks.
      iterator1.close();
    }

    try {
      for(iterator2.seekToFirst(); iterator2.hasNext(); iterator2.next()) {
        var key = SerializationUtils.deserialize(iterator2.peekNext().getKey());
        var value = iterator2.peekNext().getValue();
        if (key instanceof PartitionKey)
        {
          mapOfAdjListPrimaryKeys.put( ((PartitionKey) key).getPartitionKey(), Utils.cborDecoderSet(value));
        }
        else
        {
          mapOfPrimaryKeysWithTimestampMetrics.put(key, SerializationUtils.deserialize(value));
        }

      }
    } finally {
      iterator2.close();
    }
    System.out.println("Total partition keys:" + mapOfAdjListPrimaryKeys.keySet().stream().count());
    System.out.println("Total primary keys in the ledger:" + mapOfPrimaryKeysWithTimestampMetrics.keySet().stream().count());

    Map<String, Long> primaryKeysPerPartition = new HashMap<>();

    mapOfAdjListPrimaryKeys.entrySet().forEach(
            (x) -> primaryKeysPerPartition.put(x.getKey(), x.getValue().stream().count())
    );

    var rs = primaryKeysPerPartition.entrySet().stream().map(
            Map.Entry::getValue).reduce(Long::sum);

    System.out.println("Final number of primaryKeys in ledger sets:"+rs.get());

    assert rs.get() == mapOfPrimaryKeysWithTimestampMetrics.keySet().stream().count();
  }
}