package com.lolski.ignite;

import com.lolski.ignite.IgniteStorage;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.lolski.ignite.IgniteTestHelpers.getIgniteStorage;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

public class IgniteStorageTest {
    @Test
    public void shouldPopIndexProperly() {
        try (IgniteStorage igniteStorage = getIgniteStorage()) {

            Set<String> conceptIds = new HashSet<>(Arrays.asList("id1", "id2", "id3", "id4"));
            igniteStorage.addIndex("keyspace", "attribute-name-value-adam", conceptIds);

            String pop = igniteStorage.popIndex("keyspace");
            String popAgain = igniteStorage.popIndex("keyspace");
            assertThat(pop, equalTo("attribute-name-value-adam"));
            assertThat(popAgain, nullValue());
        }
    }

    @Test
    public void shouldPopIdsProperly() {
        try (IgniteStorage igniteStorage = getIgniteStorage()) {

            Set<String> conceptIds = new HashSet<>(Arrays.asList("id1", "id2"));
            igniteStorage.addIndex("keyspace", "attribute-name-value-adam", conceptIds);

            Set<String> pop = igniteStorage.popIds("keyspace", "attribute-name-value-adam");
            Set<String> popAgain = igniteStorage.popIds("keyspace", "attribute-name-value-adam");
            assertThat(pop, equalTo(conceptIds));
            assertThat(popAgain, emptyIterable());
        }
    }
}
