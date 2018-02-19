package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class IgniteMapOfLongTest {
    @Test
    public void getInstanceCountMustIncrementAndReturnCountCorrectly() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMapOfLong mapOfLong = new IgniteMapOfLong(IgniteTestHelpers.getCacheMapOfLong(ignite, "a"));
            mapOfLong.incrementByTx(ignite.transactions(), "new-concept-id", 13L);
            long count = mapOfLong.get("new-concept-id");
            assertThat(count, equalTo(13L));
        }
    }

    @Test
    public void getInstanceCountMustIncrementMultipleTimesCorrectly() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMapOfLong mapOfLong = new IgniteMapOfLong(IgniteTestHelpers.getCacheMapOfLong(ignite, "a"));
            mapOfLong.incrementByTx(ignite.transactions(), "new-concept-id", 1L);
            mapOfLong.incrementByTx(ignite.transactions(), "new-concept-id", 2L);
            mapOfLong.incrementByTx(ignite.transactions(), "new-concept-id", -1L);
            long count = mapOfLong.get("new-concept-id");
            assertThat(count, equalTo(2L));
        }
    }

    @Test
    public void getInstanceCountMustReturnZero_ifConceptIdDoesNotExist() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMapOfLong mapOfLong = new IgniteMapOfLong(IgniteTestHelpers.getCacheMapOfLong(ignite, "a"));
            long count = mapOfLong.get("non-existing-concept-id");
            assertThat(count, equalTo(0L));
        }
    }

    @Test
    public void getShardCountMustReturnZero_ifConceptIdDoesNotExist() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMapOfLong mapOfLong = new IgniteMapOfLong(IgniteTestHelpers.getCacheMapOfLong(ignite, "a"));
            long count = mapOfLong.get("non-existing-concept-id");
            assertThat(count, equalTo(0L));
        }
    }
}
