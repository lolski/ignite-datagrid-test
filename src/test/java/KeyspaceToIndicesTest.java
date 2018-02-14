import com.lolski.ignite.KeyspaceDoesNotExistInKeyspaceToIndicesException;
import com.lolski.ignite.KeyspaceToIndices;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.junit.Test;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

public class KeyspaceToIndicesTest {
    @Test
    public void shouldPutCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            KeyspaceToIndices keyspaceToIndices = new KeyspaceToIndices().start(ignite);

            keyspaceToIndices.put(ignite.transactions(), "keyspace", "1");
            keyspaceToIndices.put(ignite.transactions(), "keyspace", "2");

            assertThat(keyspaceToIndices.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1", "2"))));
        }
    }

    @Test
    public void shouldNotHaveDuplicate() {
        try (Ignite ignite = Ignition.start()) {
            KeyspaceToIndices keyspaceToIndices = new KeyspaceToIndices().start(ignite);

            keyspaceToIndices.put(ignite.transactions(), "keyspace", "1");
            keyspaceToIndices.put(ignite.transactions(), "keyspace", "1");

            assertThat(keyspaceToIndices.getAll("keyspace"), equalTo(new TreeSet<>(Arrays.asList("1"))));
        }
    }

    @Test
    public void shouldPopNonEmptyCollectionCorrectly() {
        try (Ignite ignite = Ignition.start()) {
            KeyspaceToIndices keyspaceToIndices = new KeyspaceToIndices().start(ignite);

            // initialize with 2 element, and pop one of them
            keyspaceToIndices.put(ignite.transactions(), "keyspace", "1");
            keyspaceToIndices.put(ignite.transactions(), "keyspace", "2");
            String pop = keyspaceToIndices.pop(ignite.transactions(), "keyspace");

            SortedSet<String> setWithoutThePoppedElement = new TreeSet<>(Arrays.asList("1", "2"));
            setWithoutThePoppedElement.remove(pop);

            assertThat(keyspaceToIndices.getAll("keyspace"), equalTo(setWithoutThePoppedElement));
        }
    }

    @Test(expected = KeyspaceDoesNotExistInKeyspaceToIndicesException.class)
    public void shouldThrow_whenPoppingFromANonExistingKeyspace() {
        try (Ignite ignite = Ignition.start()) {
            KeyspaceToIndices emptyKeyspaceToIndicesCollection = new KeyspaceToIndices().start(ignite);

            String pop = emptyKeyspaceToIndicesCollection.pop(ignite.transactions(), "keyspace");

            SortedSet<String> setWithoutThePoppedElement = new TreeSet<>(Arrays.asList("1", "2"));
            setWithoutThePoppedElement.remove(pop);

            assertThat(emptyKeyspaceToIndicesCollection.getAll("keyspace"), equalTo(setWithoutThePoppedElement));
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrow_whenPoppingFromAnAlreadyEmptyCollection() {
        // initialize with 1 element, and pop twice
        try (Ignite ignite = Ignition.start()) {
            KeyspaceToIndices keyspaceToIndices = new KeyspaceToIndices().start(ignite);

            keyspaceToIndices.put(ignite.transactions(), "keyspace", "1");

            keyspaceToIndices.pop(ignite.transactions(), "keyspace");
            keyspaceToIndices.pop(ignite.transactions(), "keyspace");
        }
    }
}
