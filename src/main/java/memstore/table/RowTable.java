package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * RowTable, which stores data in row-major format.
 * That is, data is laid out like
 * row 1 | row 2 | ... | row n.
 */
public class RowTable implements Table {
    protected int numCols;
    protected int numRows;
    protected ByteBuffer rows;

    public RowTable() {
    }

    private int getOffset(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return offset;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        // TODO: Implement this!
        int offset = getOffset(rowId, colId);
        return this.rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        // TODO: Implement this!
        int offset = getOffset(rowId, colId);
        this.rows.putInt(offset, field);
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
        for (int rowId = 0; rowId < this.numRows; rowId++) {
            if (getIntField(rowId, 1) > threshold1
                    && getIntField(rowId, 2) < threshold2) {
                sum += getIntField(rowId, 0);
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
        for (int rowId = 0; rowId < numRows; rowId++) {
            if (getIntField(rowId, 0) > threshold) {
                for (int colId = 0; colId < numCols; colId++) {
                    sum += getIntField(rowId, colId);
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
        for (int rowId = 0; rowId < numRows; rowId++) {
            if (getIntField(rowId, 0) < threshold) {
                putIntField(rowId, 3, getIntField(rowId, 1) + getIntField(rowId, 2));
                cnt++;
            }
        }
        return cnt;
    }
}
