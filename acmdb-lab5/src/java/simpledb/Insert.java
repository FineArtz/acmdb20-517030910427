package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private TupleDesc tupleDesc;
    private boolean hasBeenCalled;

    /**
     * Constructor.
     *
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableId)
    throws DbException {
        this.tupleDesc = child.getTupleDesc();
        if (!tupleDesc.equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException(String.format(
                    "Transaction %d: cannot insert tuple with description %s into table %d: type mismatched.",
                    t.getId(), tupleDesc.toString(), tableId));
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        this.hasBeenCalled = false;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open()
    throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind()
    throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the constructor. It returns a one field tuple
     * containing the number of inserted records. Inserts should be passed through BufferPool. An instances of
     * BufferPool is available via Database.getBufferPool(). Note that insert DOES NOT need check to see if a particular
     * tuple is a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or null if called more than once.
     *
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext()
    throws TransactionAbortedException, DbException {
        if (this.hasBeenCalled) {
            return null;
        }
        int cnt = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
                ++cnt;
            }
            catch (IOException e) {
                throw new DbException(String.format(
                        "IO Exception occurred when inserting tuple %d.", tuple.getRecordId().hashCode()));
            }
        }
        Tuple ret = new Tuple(tupleDesc);
        ret.setField(0, new IntField(cnt));
        hasBeenCalled = true;
        return ret;
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
