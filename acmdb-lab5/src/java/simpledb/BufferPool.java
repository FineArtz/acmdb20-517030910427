package simpledb;

import javafx.util.Pair;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private class HitRate implements Comparable<HitRate> {
        private PageId pageId;
        private int hitCount;
        private long hitTime;

        public HitRate(PageId pageId, int hitCount, long hitTime) {
            this.pageId = pageId;
            this.hitCount = hitCount;
            this.hitTime = hitTime;
        }

        @Override
        public int compareTo(HitRate o) {
            int cmp = Integer.compare(hitCount, o.hitCount);
            if (cmp == 0) {
                return Long.compare(hitTime, o.hitTime);
            }
            else {
                return cmp;
            }
        }
    }

    private int numPages;
    private Map<PageId, Page> pageMap = new ConcurrentHashMap<>();
    private Map<PageId, HitRate> LFUCount = new ConcurrentHashMap<>();

    public enum LockType implements Serializable {
        SHARED, EXCLUSIVE;
        public static LockType getLock(int i) {
            return values()[i];
        }
    }

    private class Lock {
        private TransactionId tid;
        private LockType lockType;

        public Lock(TransactionId tid, LockType lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }

        public void setLockType(LockType lockType) {
            this.lockType = lockType;
        }

        public TransactionId getTid() {
            return tid;
        }

        public LockType getLockType() {
            return lockType;
        }
    }

    private Map<PageId, List<Lock>> pageLockMap = new ConcurrentHashMap<>();
    // Locks are on granularity of pages, therefor transactionLockMap is redundant.
    // private Map<PageId, Lock> transactionLockMap = new ConcurrentHashMap<>();

    private synchronized boolean acquireLock(TransactionId tid, PageId pid, LockType lockType) {
        if (!pageLockMap.containsKey(pid)) {
            // No lock on the page, simply add a new lock.
            Lock lock = new Lock(tid, lockType);
            pageLockMap.put(pid, new ArrayList<>(Collections.singleton(lock)));
            return true;
        }

        List<Lock> lockList = pageLockMap.get(pid);
        assert !lockList.isEmpty();
        for (Lock lock : lockList) {
            if (lock.getTid().equals(tid)) {
                if (lock.getLockType() == lockType || lock.getLockType() == LockType.EXCLUSIVE) {
                    return true;
                }
                if (lockList.size() == 1) {
                    // Upgrade the lock to EXCLUSIVE.
                    lock.setLockType(LockType.EXCLUSIVE);
                    return true;
                }
                // Already holds a shared lock, but requires an exclusive lock.
                return false;
            }
        }

        if (lockList.get(0).getLockType() == LockType.EXCLUSIVE) {
            // This must be an exclusive lock
            assert lockList.size() == 1;
            return false;
        }
        if (lockType == LockType.SHARED) {
            Lock lock = new Lock(tid, LockType.SHARED);
            lockList.add(lock);
            pageLockMap.put(pid, lockList);
            return true;
        }

        // Require an exclusive lock, but shared lock(s) has already existed.
        return false;
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm, int debug)
        throws TransactionAbortedException, DbException {
        boolean success =
                acquireLock(tid, pid, perm == Permissions.READ_ONLY ? LockType.SHARED : LockType.EXCLUSIVE);
        if (!success) {
            throw new TransactionAbortedException();
        }

        Page page = pageMap.get(pid);
        if (page == null) {
            while (pageMap.size() > numPages) {
                evictPage();
            }
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageMap.put(pid, page);
            LFUCount.put(pid, new HitRate(pid, 0, System.nanoTime()));
        }
        addHitCount(pid);
        return page;
    }
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        return getPage(tid, pid, perm, -1);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        assert pageLockMap.containsKey(pid);
        List<Lock> lockList = pageLockMap.get(pid);
        Iterator<Lock> lockIterator = lockList.iterator();
        synchronized (lockIterator) {
            while (lockIterator.hasNext()) {
                Lock lock = lockIterator.next();
                if (lock.getTid().equals(tid)) {
                    lockIterator.remove();
                    if (lockList.isEmpty()) {
                        pageLockMap.remove(pid);
                    }
                    return;
                }
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        synchronized (pageLockMap) {
            if (pageLockMap.containsKey(p)) {
                List<Lock> lockList = pageLockMap.get(p);
                for (Lock lock : lockList) {
                    if (lock.getTid().equals(tid)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if (commit) {
            flushPages(tid);
        }
        else {
            synchronized (pageMap) {
                for (PageId pid : pageMap.keySet()) {
                    Page page = pageMap.get(pid);
                    if (page.isDirty().equals(tid)) {
                        pageMap.put(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
                    }
                }
            }
        }
        for (PageId pid : pageMap.keySet()) {
            if (holdsLock(tid, pid)) {
                releasePage(tid, pid);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pageArrayList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : pageArrayList) {
            PageId pageId = page.getId();
            if (!pageMap.containsKey(pageId)) {
                while (pageMap.size() >= numPages) {
                    evictPage();
                }
            }
            page.markDirty(true, tid);
            pageId = page.getId();
            pageMap.put(pageId, page);
            LFUCount.put(pageId, new HitRate(pageId, 1, System.nanoTime()));
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        ArrayList<Page> pageArrayList = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        for (Page page : pageArrayList) {
            PageId pageId = page.getId();
            if (!pageMap.containsKey(pageId)) {
                while (pageMap.size() >= numPages) {
                    evictPage();
                }
            }
            page.markDirty(true, tid);
            pageId = page.getId();
            pageMap.put(pageId, page);
            LFUCount.put(pageId, new HitRate(pageId, 1, System.nanoTime()));
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pageId : pageMap.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        if (pageMap.containsKey(pid)) {
            try {
                flushPage(pid);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            pageMap.remove(pid);
            LFUCount.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pageMap.get(pid);
        try {
            page.markDirty(false, null);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
        catch (Exception e) {
            throw new IOException();
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Page page : pageMap.values()) {
            if (page.isDirty().equals(tid)) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        HitRate LFUPage = Collections.min(LFUCount.values());
        try {
            flushPage(LFUPage.pageId);
        }
        catch (IOException e) {
            throw new DbException(String.format("IOException occurred when flushing page %d.", LFUPage.pageId.hashCode()));
        }
        pageMap.remove(LFUPage.pageId);
        LFUCount.remove(LFUPage.pageId);
    }

    private void addHitCount(PageId pageId) {
        HitRate hitRate = LFUCount.get(pageId);
        if (hitRate == null) {
            return;
        }
        ++hitRate.hitCount;
        hitRate.hitTime = System.nanoTime();
    }
}
