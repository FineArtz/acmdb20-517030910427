package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private final TupleDesc tupleDesc;
    private DbIterator child;
    private boolean hasBeenCalled;

    /**
     * Constructor specifying the transaction that this delete belongs to as well as the child to read from.
     *
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are processed via the buffer pool (which can be
     * accessed via the Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     *
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext()
    throws TransactionAbortedException, DbException {
        if (hasBeenCalled) {
            return null;
        }
        int cnt = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().deleteTuple(tid, tuple);
                ++cnt;
            }
            catch (IOException e) {
                throw new DbException(String.format(
                        "IO Exception occurred when deleting tuple %d.", tuple.getRecordId().hashCode()));
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
