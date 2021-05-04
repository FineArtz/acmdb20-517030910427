package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbField;
    private final Type gbFieldType;
    private final int agField;
    private final Op what;
    private final Map<Field, Integer> groups = new HashMap<>();
    private final Map<Field, Integer> avgCount = new HashMap<>();

    /**
     * Aggregate constructor
     *
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no
     *         grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.agField = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gpValue = gbField == NO_GROUPING ? null : tup.getField(gbField);
        Integer agValue = Integer.parseInt(tup.getField(agField).toString());
        Integer oldValue = groups.get(gpValue);
        Integer newValue = null;
        switch (what) {
            case MIN:
                newValue = (oldValue == null ? agValue : Math.min(agValue, oldValue));
                break;
            case MAX:
                newValue = (oldValue == null ? agValue : Math.max(agValue, oldValue));
                break;
            case AVG:
                Integer cnt = avgCount.get(gpValue);
                avgCount.put(gpValue, oldValue == null ? 1 : cnt + 1);
            case SUM:
                newValue = (oldValue == null ? agValue : agValue + oldValue);
                break;
            case COUNT:
                newValue = (oldValue == null ? 1 : oldValue + 1);
                break;
        }
        groups.put(gpValue, newValue);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if using group, or a single
     *         (aggregateVal) if no grouping. The aggregateVal is determined by the type of aggregate specified in the
     *         constructor.
     */
    public DbIterator iterator() {
        boolean NO_GROUP = gbField == NO_GROUPING;
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc tupleDesc = Utility.getAggregateTupleDesc(gbFieldType, NO_GROUP);
        for (Map.Entry<Field, Integer> entry : groups.entrySet()) {
            Field gpValue = entry.getKey();
            Integer agValue = entry.getValue();
            /*
             * Note that dynamically computing the average may cause decimal loss.
             * A simple solution is computing it at final only once.
             */
            if (what == Op.AVG) {
                agValue /= avgCount.get(gpValue);
            }
            tuples.add(Utility.getAggregateTuple(tupleDesc, NO_GROUP, gpValue, agValue));
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
