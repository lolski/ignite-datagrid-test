import com.lolski.ignite.SetDoesNotExistException;
import com.lolski.ignite.IgniteMultiMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

public class IgniteMultiMapTest {
    @Test
    public void shouldDoPutTxCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap("a-random-cache-name").getOrCreate(ignite);

            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "2");

            assertThat(igniteMultiMap.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldDoPutCorrectlyWhenGivenASetOfElements() {
        try (Ignite ignite = Ignition.start()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap("a-random-cache-name").getOrCreate(ignite);

            igniteMultiMap.putAll("keyspace", new TreeSet<>(Arrays.asList("1", "2")));

            assertThat(igniteMultiMap.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldNotHaveDuplicate() {
        try (Ignite ignite = Ignition.start()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap("a-random-cache-name").getOrCreate(ignite);

            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");

            assertThat(igniteMultiMap.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1"))));
        }
    }

    @Test
    public void shouldPopNonEmptySetCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap("a-random-cache-name").getOrCreate(ignite);

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
        try (Ignite ignite = Ignition.start()) {
            IgniteMultiMap emptyIgniteMultiMapCollection = new IgniteMultiMap("a-random-cache-name").getOrCreate(ignite);

            String pop = emptyIgniteMultiMapCollection.popOneTx(ignite.transactions(), "keyspace");

            SortedSet<String> setWithoutThePoppedElement = new TreeSet<>(Arrays.asList("1", "2"));
            setWithoutThePoppedElement.remove(pop);

            assertThat(emptyIgniteMultiMapCollection.getAll("keyspace"), equalTo(setWithoutThePoppedElement));
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrow_whenPoppingFromAnAlreadyEmptyCollection() {
        // initialize with 1 element, and popOneTx twice
        try (Ignite ignite = Ignition.start()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap("a-random-cache-name").getOrCreate(ignite);

            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace", "1");

            igniteMultiMap.popOneTx(ignite.transactions(), "keyspace");
            igniteMultiMap.popOneTx(ignite.transactions(), "keyspace");
        }
    }

    @Test
    public void shouldPopAll_fromNonEmptySetCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap("a-random-cache-name").getOrCreate(ignite);

            igniteMultiMap.putAll("keyspace", new TreeSet<>(Arrays.asList("1", "2")));
            Set<String> popAll = igniteMultiMap.popAllTx(ignite.transactions(), "keyspace");

            assertThat(igniteMultiMap.getAll("keyspace"), equalTo(new TreeSet<>()));
            assertThat(popAll, equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldGetAllKeysCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            IgniteMultiMap igniteMultiMap = new IgniteMultiMap("a-random-cache-name").getOrCreate(ignite);

            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace1", "1");
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace2", "1");
            igniteMultiMap.putOneTx(ignite.transactions(), "keyspace3", "1");

            assertThat(igniteMultiMap.getKeys(), equalTo(new TreeSet<>(Arrays.asList("keyspace1", "keyspace2", "keyspace3"))));
        }

    }
}
