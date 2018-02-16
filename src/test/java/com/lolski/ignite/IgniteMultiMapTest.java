package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

public class IgniteMultiMapTest {
    @Test
    public void shouldDoPutTxCorrectly() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));

            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "2");

            assertThat(igniteMultiMap.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldDoPutCorrectlyWhenGivenASetOfElements() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));

            igniteMultiMap.putAll("keyspace", new TreeSet<>(Arrays.asList("1", "2")));

            assertThat(igniteMultiMap.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldNotHaveDuplicate() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));

            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");

            assertThat(igniteMultiMap.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1"))));
        }
    }

    @Test
    public void shouldPopNonEmptySetCorrectly() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));

            // initialize with 2 element, and popOneTx one of them
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "2");
            String pop = igniteMultiMap.popOneTx(ignite.transactions(), "keyspace");

            SortedSet<String> setWithoutThePoppedElement = new TreeSet<>(Arrays.asList("1", "2"));
            setWithoutThePoppedElement.remove(pop);

            assertThat(igniteMultiMap.getAll("keyspace"), equalTo(setWithoutThePoppedElement));
        }
    }

    @Test(expected = SetDoesNotExistException.class)
    public void shouldThrow_whenPoppingFromANonExistingKeyspace() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap emptyIgniteMultiMapCollection = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));

            String pop = emptyIgniteMultiMapCollection.popOneTx(ignite.transactions(), "keyspace");

            SortedSet<String> setWithoutThePoppedElement = new TreeSet<>(Arrays.asList("1", "2"));
            setWithoutThePoppedElement.remove(pop);

            assertThat(emptyIgniteMultiMapCollection.getAll("keyspace"), equalTo(setWithoutThePoppedElement));
        }
    }

    @Test(expected = SetDoesNotExistException.class)
    public void shouldThrow_whenPoppingFromAnAlreadyEmptyCollection() {
        // initialize with 1 element, and popOneTx twice
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));

            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");

            igniteMultiMap.popOneTx(ignite.transactions(), "keyspace");
            igniteMultiMap.popOneTx(ignite.transactions(), "keyspace");
        }
    }

    @Test
    public void shouldPopAll_fromNonEmptySetCorrectly() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));

            igniteMultiMap.putAll("keyspace", new TreeSet<>(Arrays.asList("1", "2")));
            Set<String> popAll = igniteMultiMap.popAllTx(ignite.transactions(), "keyspace");

            assertThat(igniteMultiMap.getAll("keyspace"), nullValue());
            assertThat(popAll, equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test(expected = SetDoesNotExistException.class)
    public void shouldPopAll_fromANonExistingKeyspaceCorrectly() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));

            igniteMultiMap.popAllTx(ignite.transactions(), "keyspace");
        }
    }

    @Test(expected = SetDoesNotExistException.class)
    public void shouldPopAll_fromAnEmptyKeyspaceCorrectly() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");
            igniteMultiMap.popOneTx(ignite.transactions(), "keyspace");
            igniteMultiMap.popAllTx(ignite.transactions(), "keyspace");
            assertThat(igniteMultiMap.getAll("keyspace"), nullValue());
        }
    }

    @Test
    public void shouldGetAllKeysCorrectly() {
        try (Ignite ignite = IgniteTestHelpers.getIgnite()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap(IgniteTestHelpers.getCache(ignite, "a-random-cache-name"));

            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace1", "1");
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace2", "1");
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace3", "1");

            assertThat(igniteMultiMap.getKeys(), equalTo(new TreeSet<>(Arrays.asList("keyspace1", "keyspace2", "keyspace3"))));
        }

    }
}
