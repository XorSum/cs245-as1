package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 * row 1 | row 2 | ... | row n.
 * <p>
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        // TODO: Implement this!
        this.index = new TreeMap<Integer, IntArrayList>();
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        this.numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                int field = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                this.rows.putInt(offset, field);
                if (colId == this.indexColumn) {
                    IntArrayList value;
                    if (this.index.containsKey(field)) {
                        value = this.index.get(field);
                    } else {
                        value = new IntArrayList();
                    }
                    value.add(rowId);
                    this.index.put(field, value);
                }
            }
        }
    }

    private int getOffset(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return offset;
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        // TODO: Implement this!
        int offset = getOffset(rowId, colId);
        int value = this.rows.getInt(offset);
        return value;
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        // TODO: Implement this!
        int offset = getOffset(rowId, colId);
        int origin = this.rows.getInt(offset);
        this.rows.putInt(offset, field);
        if (colId == this.indexColumn) {
            IntArrayList value = this.index.get(origin);
            value.rem(rowId);
            this.index.put(origin, value);
            value = this.index.get(field);
            if (value == null) {
                value = new IntArrayList();
            }
            value.add(rowId);
            this.index.put(field, value);
        }
    }

    /**
     * Implements the query
     * SELECT SUM(col0) FROM table;
     * <p>
     * Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        // TODO: Implement this!
        long sum = 0;
        for (int rowId = 0; rowId < this.numRows; rowId++) {
            sum += getIntField(rowId, 0);
        }
        return sum;
    }

    /**
     * Implements the query
     * SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     * <p>
     * Returns the sum of all elements in the first column of the table,
     * subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        // TODO: Implement this!
        long sum = 0;
        if (this.indexColumn == 1) {
            for (int field: this.index.tailMap(threshold1+1).keySet()) {
                for (int rowId : this.index.get(field)) {
                    if (getIntField(rowId, 2) < threshold2) {
                        sum += getIntField(rowId, 0);
                    }
                }
            }
        } else if (this.indexColumn == 2) {
            for (int field : this.index.headMap(threshold2).keySet()) {
                for (int rowId : this.index.get(field)) {
                    if (getIntField(rowId, 1) > threshold1) {
                        sum += getIntField(rowId, 0);
                    }
                }
            }
        } else {
            for (int rowId = 0; rowId < this.numRows; rowId++) {
                if (getIntField(rowId, 1) > threshold1
                        && getIntField(rowId, 2) < threshold2) {
                    sum += getIntField(rowId, 0);
                }
            }
        }
        return sum;
    }

    /**
     * Implements the query
     * SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     * <p>
     * Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        // TODO: Implement this!
        long sum = 0;
        if (this.indexColumn == 0) {
            for (int key : this.index.tailMap(threshold+1).keySet()) {
                for (int rowId : this.index.get(key)) {
                    for (int colId = 0; colId < numCols; colId++) {
                        sum += getIntField(rowId, colId);
                    }
                }
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (getIntField(rowId, 0) > threshold) {
                    for (int colId = 0; colId < numCols; colId++) {
                        sum += getIntField(rowId, colId);
                    }
                }
            }
        }
        return sum;
    }

    /**
     * Implements the query
     * UPDATE(col3 = col1 + col2) WHERE col0 < threshold;
     * <p>
     * Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        // TODO: Implement this!

        int cnt = 0;
        if (this.indexColumn == 0) {
            for (int key : this.index.headMap(threshold).keySet()) {
                for (int rowId : this.index.get(key)) {
                    putIntField(rowId, 3, getIntField(rowId, 1) + getIntField(rowId, 2));
                    cnt++;
                }
            }
        }  else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (getIntField(rowId, 0) < threshold) {
                    putIntField(rowId, 3, getIntField(rowId, 1) + getIntField(rowId, 2));
                    cnt++;
                }
            }
        }
        return cnt;
    }
}
