import com.lolski.ignite.SetDoesNotExistException;
import com.lolski.ignite.PushPopSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

public class PushPopSetTest {
    @Test
    public void shouldDoPutTxCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            PushPopSet pushPopSet = new PushPopSet("a-random-cache-name").getOrCreate(ignite);

            pushPopSet.putTx(ignite.transactions(), "keyspace", "1");
            pushPopSet.putTx(ignite.transactions(), "keyspace", "2");

            assertThat(pushPopSet.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldDoPutCorrectlyWhenGivenASetOfElements() {
        try (Ignite ignite = Ignition.start()) {
            PushPopSet pushPopSet = new PushPopSet("a-random-cache-name").getOrCreate(ignite);

            pushPopSet.put("keyspace", new TreeSet<>(Arrays.asList("1", "2")));

            assertThat(pushPopSet.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldNotHaveDuplicate() {
        try (Ignite ignite = Ignition.start()) {
            PushPopSet pushPopSet = new PushPopSet("a-random-cache-name").getOrCreate(ignite);

            pushPopSet.putTx(ignite.transactions(), "keyspace", "1");
            pushPopSet.putTx(ignite.transactions(), "keyspace", "1");

            assertThat(pushPopSet.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1"))));
        }
    }

    @Test
    public void shouldPopNonEmptySetCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            PushPopSet pushPopSet = new PushPopSet("a-random-cache-name").getOrCreate(ignite);

            // initialize with 2 element, and pop one of them
            pushPopSet.putTx(ignite.transactions(), "keyspace", "1");
            pushPopSet.putTx(ignite.transactions(), "keyspace", "2");
            String pop = pushPopSet.pop(ignite.transactions(), "keyspace");

            SortedSet<String> setWithoutThePoppedElement = new TreeSet<>(Arrays.asList("1", "2"));
            setWithoutThePoppedElement.remove(pop);

            assertThat(pushPopSet.getAll("keyspace"), equalTo(setWithoutThePoppedElement));
        }
    }

    @Test(expected = SetDoesNotExistException.class)
    public void shouldThrow_whenPoppingFromANonExistingKeyspace() {
        try (Ignite ignite = Ignition.start()) {
            PushPopSet emptyPushPopSetCollection = new PushPopSet("a-random-cache-name").getOrCreate(ignite);

            String pop = emptyPushPopSetCollection.pop(ignite.transactions(), "keyspace");

            SortedSet<String> setWithoutThePoppedElement = new TreeSet<>(Arrays.asList("1", "2"));
            setWithoutThePoppedElement.remove(pop);

            assertThat(emptyPushPopSetCollection.getAll("keyspace"), equalTo(setWithoutThePoppedElement));
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrow_whenPoppingFromAnAlreadyEmptyCollection() {
        // initialize with 1 element, and pop twice
        try (Ignite ignite = Ignition.start()) {
            PushPopSet pushPopSet = new PushPopSet("a-random-cache-name").getOrCreate(ignite);

            pushPopSet.putTx(ignite.transactions(), "keyspace", "1");

            pushPopSet.pop(ignite.transactions(), "keyspace");
            pushPopSet.pop(ignite.transactions(), "keyspace");
        }
    }

    @Test
    public void shouldPopAll_fromNonEmptySetCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            PushPopSet pushPopSet = new PushPopSet("a-random-cache-name").getOrCreate(ignite);

            pushPopSet.put("keyspace", new TreeSet<>(Arrays.asList("1", "2")));
            Set<String> popAll = pushPopSet.popAll(ignite.transactions(), "keyspace");

            assertThat(pushPopSet.getAll("keyspace"), equalTo(new TreeSet<>()));
            assertThat(popAll, equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldGetAllKeysCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            PushPopSet pushPopSet = new PushPopSet("a-random-cache-name").getOrCreate(ignite);

            pushPopSet.putTx(ignite.transactions(), "keyspace1", "1");
            pushPopSet.putTx(ignite.transactions(), "keyspace2", "1");
            pushPopSet.putTx(ignite.transactions(), "keyspace3", "1");

            assertThat(pushPopSet.getKeys(), equalTo(new TreeSet<>(Arrays.asList("keyspace1", "keyspace2", "keyspace3"))));
        }

    }
}
