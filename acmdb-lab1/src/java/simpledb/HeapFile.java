package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    private class HeapFileIterator implements DbFileIterator {

        private TransactionId transactionId;
        private int pageNo, tableId;
        private Iterator<Tuple> tupleIterator;

        HeapFileIterator(TransactionId tid) {
            this.transactionId = tid;
            this.pageNo = 0;
            this.tableId = getId();
            this.tupleIterator = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNo = 0;
            PageId pageId = new HeapPageId(tableId, pageNo);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(
                    transactionId, pageId, Permissions.READ_WRITE);
            tupleIterator = heapPage.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return tupleIterator != null && (tupleIterator.hasNext() || pageNo < numPages() - 1);
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator == null) {
                throw new NoSuchElementException("No tuple iterator.");
            }
            if (!tupleIterator.hasNext()) {
                ++pageNo;
                PageId pageId = new HeapPageId(tableId, pageNo);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(
                        transactionId, pageId, Permissions.READ_WRITE);
                tupleIterator = heapPage.iterator();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            try {
                close();
                open();
            }
            catch (Exception e) {
                throw new DbException("");
            }
        }

        @Override
        public void close() {
            pageNo = 0;
            tupleIterator = null;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pgNo = pid.pageNumber();
        int len = BufferPool.getPageSize();
        long offset = pgNo * len;
        if (len + offset > file.length()) {
            throw new IllegalArgumentException(String.format(
                    "Page %d does not exist in file %d.", pid.pageNumber(), getId()));
        }
        byte[] data = new byte[len];
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            raf.readFully(data, 0, len);
            return new HeapPage((HeapPageId) pid, data);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(String.format(
                    "IO exception occurred when reading page %d of file %d.", pid.pageNumber(), getId()));
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) ((double) file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

