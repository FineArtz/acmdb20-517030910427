package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only support aggregates
 * over a single column, grouped by a single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private final int agField;
    private final int gbField;
    private final Aggregator.Op what;
    private final Aggregator aggregator;
    private DbIterator child;
    private DbIterator agIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to construct an {@link IntegerAggregator} or
     * {@link StringAggregator} to help you with your implementation of readNext().
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.agField = afield;
        this.gbField = gfield;
        this.what = aop;
        if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield),
                                               afield, aop);
        }
        else {
            aggregator = new StringAggregator(gfield, gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield),
                                              afield, aop);
        }
        this.agIterator = null;
    }

    @SuppressWarnings("unused")
    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby field index in the <b>INPUT</b> tuples.
     *         If not, return {@link simpledb.Aggregator#NO_GROUPING}
     */
    int groupField() {
        return gbField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name of the groupby field in the <b>OUTPUT</b>
     *         tuples If not, return null;
     */
    String groupFieldName() {
        return child.getTupleDesc().getFieldName(gbField);
    }

    /**
     * @return the aggregate field
     */
    @SuppressWarnings("unused")
    public int aggregateField() {
        return agField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     */
    String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(agField);
    }

    /**
     * @return return the aggregate operator
     */
    Aggregator.Op aggregateOp() {
        return what;
    }

    public void open()
    throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();
        while (child.hasNext()) {
            Tuple tuple = child.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }
        agIterator = aggregator.iterator();
        agIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
     * and the second field is the result of computing the aggregate, If there is no group by field, then the result
     * tuple should contain one field representing the result of the aggregate. Should return null if there are no more
     * tuples.
     */
    protected Tuple fetchNext()
    throws TransactionAbortedException, DbException {
        return agIterator.hasNext() ? agIterator.next() : null;
    }

    public void rewind()
    throws DbException, TransactionAbortedException {
        agIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field, this will have one field - the aggregate
     * column. If there is a group by field, the first field will be the group by field, and the second will be the
     * aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example: "aggName(aop)
     * (child_td.getFieldName(afield))" where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator.
     */
    public TupleDesc getTupleDesc() {
        return new TupleDesc(
                gbField == Aggregator.NO_GROUPING ?
                new Type[] {Type.INT_TYPE} :
                new Type[] {child.getTupleDesc().getFieldType(gbField), Type.INT_TYPE},
                gbField == Aggregator.NO_GROUPING ?
                new String[] {String.format("%s (%s)", what.toString(), child.getTupleDesc().getFieldName(agField))} :
                new String[] {
                        child.getTupleDesc().getFieldName(gbField),
                        String.format("%s (%s)", what.toString(), child.getTupleDesc().getFieldName(agField))}
        );
    }

    public void close() {
        child.close();
        agIterator.close();
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }

}
