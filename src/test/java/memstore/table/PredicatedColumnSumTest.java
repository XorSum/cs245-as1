package memstore.table;

import memstore.data.CSVLoader;
import memstore.data.DataLoader;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests the correctness of the different table implementations for
 * the `predicatedColumnSum` query.
 */
public class PredicatedColumnSumTest {
    DataLoader dl;

    public PredicatedColumnSumTest() {
        dl = new CSVLoader(
                "src/main/resources/test.csv",
                5
        );
    }

    @Test
    public void testRowTable() throws IOException {
        RowTable rt = new RowTable();
        rt.load(dl);
        assertEquals(49, rt.predicatedColumnSum(3, 5));
    }

    @Test
    public void testColumnTable() throws IOException {
        ColumnTable ct = new ColumnTable();
        ct.load(dl);
        assertEquals(49, ct.predicatedColumnSum(3, 5));
    }

    @Test
    public void testIndexedTable() throws IOException {
        IndexedRowTable it0 = new IndexedRowTable(0);
        it0.load(dl);
        assertEquals(49, it0.predicatedColumnSum(3, 5));

        IndexedRowTable it1 = new IndexedRowTable(1);
        it1.load(dl);
        assertEquals(49, it1.predicatedColumnSum(3, 5));


        IndexedRowTable it2 = new IndexedRowTable(2);
        it2.load(dl);
        assertEquals(49, it2.predicatedColumnSum(3, 5));


    }
}
