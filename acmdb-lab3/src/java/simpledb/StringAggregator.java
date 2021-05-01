package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private Map<Field, Integer> groups = new HashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Operator must be COUNT for String aggregator.");
        }
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gpValue = tup.getField(gbField);
        Integer oldValue = groups.get(gpValue);
        groups.put(gpValue, oldValue == null ? 1 : oldValue + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        boolean NO_GROUP = gbField == NO_GROUPING;
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc tupleDesc = new TupleDesc(
                NO_GROUP ? new Type[] {Type.INT_TYPE} : new Type[] {gbFieldType, Type.INT_TYPE},
                NO_GROUP ? new String[] {"aggregateValue"} : new String[] {"groupValue", "aggregateValue"});
        for (Map.Entry<Field, Integer> entry : groups.entrySet()) {
            Field gpValue = entry.getKey();
            Integer agValue = entry.getValue();
            Tuple tuple = new Tuple(tupleDesc);
            if (NO_GROUP) {
                tuple.setField(0, new IntField(agValue));
            }
            else {
                tuple.setField(0, gpValue);
                tuple.setField(1, new IntField(agValue));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
