/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

public class TokenRangesDistributionTests {

    @Test
    void partitionDistribution16VnodesBy4Tiles() {
        var ranges = generateTokenRanges(10, 16);
        var rangesByTiles = Utils.getDistributedRangesByTiles(ranges, 4);
        assert rangesByTiles.size() == 4;
        var cnt = rangesByTiles.stream().mapToInt(List::size).sum();
        assert cnt == ranges.size();
    }

    @Test
    void partitionDistribution8VnodesBy4Tiles() {
        var ranges = generateTokenRanges(10, 8);
        var rangesByTiles = Utils.getDistributedRangesByTiles(ranges, 4);
        assert rangesByTiles.size() == 4;
        var cnt = rangesByTiles.stream().mapToInt(List::size).sum();
        assert cnt == ranges.size();
    }

    @Test
    void partitionDistribution8VnodesBy1Tile() {
        var ranges = generateTokenRanges(10, 8);
        var rangesByTiles = Utils.getDistributedRangesByTiles(ranges, 1);
        assert rangesByTiles.size() == 1;
        var cnt = rangesByTiles.stream().mapToInt(List::size).sum();
        assert cnt == ranges.size();
    }

    @Test
    void partitionDistribution16VnodesBy1Tiles() {
        var ranges = generateTokenRanges(1, 16);
        var rangesByTiles = Utils.getDistributedRangesByTiles(ranges, 1);
        assert rangesByTiles.size() == 1;
        var cnt = rangesByTiles.stream().mapToInt(List::size).sum();
        assert cnt == ranges.size();
    }

    List<ImmutablePair<String, String>> generateTokenRanges(int nodes, int vnodes) {
        List<ImmutablePair<String, String>> ranges = new LinkedList<>();

        int numberRanges = 0;
        while (nodes * vnodes > numberRanges ) {
            var start = new Murmur3Token(numberRanges);
            numberRanges++;
            var end = new Murmur3Token(numberRanges);
            numberRanges++;
            var token = new ImmutablePair<>(start.toString(), end.toString());
            ranges.add(token);
        }

        return ranges;
    }

}
