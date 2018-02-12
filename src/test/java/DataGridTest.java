import com.lolski.ignite.DataGrid;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

public class DataGridTest {
    @Test
    public void shouldPopIndexProperly() {
        try (DataGrid dataGrid = new DataGrid("localhost", 6666)) {
            dataGrid.start();

            Set<String> conceptIds = new HashSet<>(Arrays.asList("id1", "id2"));
            dataGrid.addIndex("keyspace", "attribute-name-value-adam", conceptIds);

            String pop = dataGrid.popIndex("keyspace");
            String popAgain = dataGrid.popIndex("keyspace");
            assertThat(pop, equalTo("attribute-name-value-adam"));
            assertThat(popAgain, nullValue());
        }
    }

    @Test
    public void shouldPopIdsProperly() {
        try (DataGrid dataGrid = new DataGrid("localhost", 6666)) {
            dataGrid.start();

            Set<String> conceptIds = new HashSet<>(Arrays.asList("id1", "id2"));
            dataGrid.addIndex("keyspace", "attribute-name-value-adam", conceptIds);

            Set<String> pop = dataGrid.popIds("keyspace", "attribute-name-value-adam");
            Set<String> popAgain = dataGrid.popIds("keyspace", "attribute-name-value-adam");
            assertThat(pop, equalTo(conceptIds));
            assertThat(popAgain, nullValue());
        }
    }
}
