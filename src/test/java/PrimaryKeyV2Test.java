import com.amazon.aws.cqlreplicator.models.PrimaryKeyV2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PrimaryKeyV2Test {

    private PrimaryKeyV2 primaryKeyV2UnderTest;

    @BeforeEach
    void setUp() {
        primaryKeyV2UnderTest = new PrimaryKeyV2(new String[]{"value1"}, new String[]{"value2"});
    }

    @Test
    void testGetSerializedPrimaryKey() {

        final ByteBuffer expectedResult = ByteBuffer.wrap(Base64.getDecoder().decode("AAAABnZhbHVlMQAAAAZ2YWx1ZTI="));

        final ByteBuffer result = primaryKeyV2UnderTest.getSerializedPrimaryKey();

        assertEquals(expectedResult, result);
    }

    @Test
    void testGetSerializedPartitionKeys() {
        // Setup
        final ByteBuffer expectedResult = ByteBuffer.wrap(Base64.getDecoder().decode("AAAABnZhbHVlMQ=="));

        // Run the test
        final ByteBuffer result = primaryKeyV2UnderTest.getSerializedPartitionKeys();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    void testGetSerializedClusteringKeys() {
        // Setup
        final ByteBuffer expectedResult = ByteBuffer.wrap(Base64.getDecoder().decode("AAAABnZhbHVlMg=="));

        // Run the test
        final ByteBuffer result = primaryKeyV2UnderTest.getSerializedClusteringKeys();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    void testGetDeserializedPartitionKeys() {
        // Setup
        final ByteBuffer key = primaryKeyV2UnderTest.getSerializedPartitionKeys();

        // Run the test
        final String[] result = primaryKeyV2UnderTest.getDeserializedPartitionKeys(key);

        // Verify the results
        assertArrayEquals(new String[]{"value1"}, result);
    }

    @Test
    void testGetDeserializedClusteringKeys() {
        // Setup
        final ByteBuffer key = primaryKeyV2UnderTest.getSerializedClusteringKeys();
        // Run the test
        final String[] result = primaryKeyV2UnderTest.getDeserializedClusteringKeys(key);

        // Verify the results
        assertArrayEquals(new String[]{"value2"}, result);
    }

    @Test
    void testGetXXHashPartitionKeys() throws Exception {
        assertEquals(4503051025529434718L, primaryKeyV2UnderTest.getXXHashPartitionKeys());
    }

    @Test
    void testGetXXHashClusteringColumns() throws Exception {
        assertEquals(1214233361398533864L, primaryKeyV2UnderTest.getXXHashClusteringColumns());
    }

    @Test
    void testGetXXHashPrimaryKey() throws Exception {
        assertEquals(7815912770163079646L, primaryKeyV2UnderTest.getXXHashPrimaryKey());
    }
}
