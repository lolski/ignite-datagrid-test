package com.lolski.ignite;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.lolski.ignite.IgniteTestHelpers.getIgniteStorage;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

public class IgniteIndexStorageTest {
    @Test
    public void shouldPopIndexProperly() {
        try (IgniteIndexStorage igniteIndexStorage = getIgniteStorage()) {

            Set<String> conceptIds = new HashSet<>(Arrays.asList("id1", "id2", "id3", "id4"));
            igniteIndexStorage.addIndex("keyspace", "attribute-name-value-adam", conceptIds);

            String pop = igniteIndexStorage.popIndex("keyspace");
            String popAgain = igniteIndexStorage.popIndex("keyspace");
            assertThat(pop, equalTo("attribute-name-value-adam"));
            assertThat(popAgain, nullValue());
        }
    }

    @Test
    public void shouldPopIdsProperly() {
        try (IgniteIndexStorage igniteIndexStorage = getIgniteStorage()) {

            Set<String> conceptIds = new HashSet<>(Arrays.asList("id1", "id2"));
            igniteIndexStorage.addIndex("keyspace", "attribute-name-value-adam", conceptIds);

            Set<String> pop = igniteIndexStorage.popIds("keyspace", "attribute-name-value-adam");
            Set<String> popAgain = igniteIndexStorage.popIds("keyspace", "attribute-name-value-adam");
            assertThat(pop, equalTo(conceptIds));
            assertThat(popAgain, emptyIterable());
        }
    }
}
