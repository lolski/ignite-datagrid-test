import com.lolski.ignite.SetDoesNotExistException;
import com.lolski.ignite.IgniteSortedSetKV;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

public class IgniteSortedSetKVTest {
    @Test
    public void shouldDoPutTxCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            IgniteSortedSetKV igniteSortedSetKV = new IgniteSortedSetKV("a-random-cache-name").getOrCreate(ignite);

            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace", "1");
            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace", "2");

            assertThat(igniteSortedSetKV.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldDoPutCorrectlyWhenGivenASetOfElements() {
        try (Ignite ignite = Ignition.start()) {
            IgniteSortedSetKV igniteSortedSetKV = new IgniteSortedSetKV("a-random-cache-name").getOrCreate(ignite);

            igniteSortedSetKV.putAll("keyspace", new TreeSet<>(Arrays.asList("1", "2")));

            assertThat(igniteSortedSetKV.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldNotHaveDuplicate() {
        try (Ignite ignite = Ignition.start()) {
            IgniteSortedSetKV igniteSortedSetKV = new IgniteSortedSetKV("a-random-cache-name").getOrCreate(ignite);

            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace", "1");
            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace", "1");

            assertThat(igniteSortedSetKV.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1"))));
        }
    }

    @Test
    public void shouldPopNonEmptySetCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            IgniteSortedSetKV igniteSortedSetKV = new IgniteSortedSetKV("a-random-cache-name").getOrCreate(ignite);

            // initialize with 2 element, and popOne one of them
            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace", "1");
            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace", "2");
            String pop = igniteSortedSetKV.popOne(ignite.transactions(), "keyspace");

            SortedSet<String> setWithoutThePoppedElement = new TreeSet<>(Arrays.asList("1", "2"));
            setWithoutThePoppedElement.remove(pop);

            assertThat(igniteSortedSetKV.getAll("keyspace"), equalTo(setWithoutThePoppedElement));
        }
    }

    @Test(expected = SetDoesNotExistException.class)
    public void shouldThrow_whenPoppingFromANonExistingKeyspace() {
        try (Ignite ignite = Ignition.start()) {
            IgniteSortedSetKV emptyIgniteSortedSetKVCollection = new IgniteSortedSetKV("a-random-cache-name").getOrCreate(ignite);

            String pop = emptyIgniteSortedSetKVCollection.popOne(ignite.transactions(), "keyspace");

            SortedSet<String> setWithoutThePoppedElement = new TreeSet<>(Arrays.asList("1", "2"));
            setWithoutThePoppedElement.remove(pop);

            assertThat(emptyIgniteSortedSetKVCollection.getAll("keyspace"), equalTo(setWithoutThePoppedElement));
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrow_whenPoppingFromAnAlreadyEmptyCollection() {
        // initialize with 1 element, and popOne twice
        try (Ignite ignite = Ignition.start()) {
            IgniteSortedSetKV igniteSortedSetKV = new IgniteSortedSetKV("a-random-cache-name").getOrCreate(ignite);

            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace", "1");

            igniteSortedSetKV.popOne(ignite.transactions(), "keyspace");
            igniteSortedSetKV.popOne(ignite.transactions(), "keyspace");
        }
    }

    @Test
    public void shouldPopAll_fromNonEmptySetCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            IgniteSortedSetKV igniteSortedSetKV = new IgniteSortedSetKV("a-random-cache-name").getOrCreate(ignite);

            igniteSortedSetKV.putAll("keyspace", new TreeSet<>(Arrays.asList("1", "2")));
            Set<String> popAll = igniteSortedSetKV.popAll(ignite.transactions(), "keyspace");

            assertThat(igniteSortedSetKV.getAll("keyspace"), equalTo(new TreeSet<>()));
            assertThat(popAll, equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldGetAllKeysCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            IgniteSortedSetKV igniteSortedSetKV = new IgniteSortedSetKV("a-random-cache-name").getOrCreate(ignite);

            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace1", "1");
            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace2", "1");
            igniteSortedSetKV.putOneTx(ignite.transactions(), "keyspace3", "1");

            assertThat(igniteSortedSetKV.getKeys(), equalTo(new TreeSet<>(Arrays.asList("keyspace1", "keyspace2", "keyspace3"))));
        }

    }
}
