package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate predicate;
    private DbIterator iterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.predicate = p;
        this.iterator = child;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        return iterator.getTupleDesc();
    }

    public void open()
    throws DbException, NoSuchElementException,
           TransactionAbortedException {
        iterator.open();
        super.open();
    }

    public void close() {
        iterator.close();
        super.close();
    }

    public void rewind()
    throws DbException, TransactionAbortedException {
        iterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     *
     * @see Predicate#filter
     */
    protected Tuple fetchNext()
    throws NoSuchElementException,
           TransactionAbortedException, DbException {
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            if (predicate.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {iterator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        iterator = children[0];
    }

}
