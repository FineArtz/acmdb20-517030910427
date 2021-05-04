package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a specified schema specified by a TupleDesc
 * object and contain Field objects with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private final List<Field> fields;
    private TupleDesc tupleDesc;
    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        tupleDesc = td;
        fields = Arrays.asList(new Field[td.numFields()]);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        fields.set(i, f);
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the system tests, the format needs to be as
     * follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int len = fields.size();
        for (int i = 0; i < len - 1; ++i) {
            sb.append(fields.get(i).toString());
            sb.append("\t");
        }
        sb.append(fields.get(len - 1).toString());
        return sb.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    @SuppressWarnings("unused")
    public Iterator<Field> fields() {
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of thi tuple
     */
    void resetTupleDesc(TupleDesc td) {
        tupleDesc = td;
    }

}
