package simpledb;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk. Access methods call into it to retrieve
 * pages, and it fetches pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches a page, BufferPool checks that the
 * transaction has the appropriate locks to read/write the page.
 *
 * @Threadsafe all fields are final
 */
public class BufferPool {
    /**
     * Default number of pages passed to the constructor. This is used by other classes. BufferPool should use the
     * numPages argument to the constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    /**
     * Bytes per page, including header.
     */
    private static final int PAGE_SIZE = 4096;
    private static int pageSize = PAGE_SIZE;
    private final int numPages;
    private final Map<PageId, Page> pageMap = new ConcurrentHashMap<>();
    private final Map<PageId, HitRate> LFUCount = new ConcurrentHashMap<>();
    private final LockManager lockManager = new LockManager();
    private final LockDependencyGraph lockDependencyGraph = new LockDependencyGraph();

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
     * Retrieve the specified page with the associated permissions. Will acquire a lock and may block if that lock is
     * held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it is present, it should be returned.  If it is
     * not present, it should be added to the buffer pool and returned.  If there is insufficient space in the buffer
     * pool, an page should be evicted and the new page should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
    throws TransactionAbortedException, DbException {
        boolean success = lockManager.acquireLock(tid, pid,
                                                  perm == Permissions.READ_ONLY ? LockType.SHARED : LockType.EXCLUSIVE);
        while (!success) {
            lockDependencyGraph.addEdge(tid, pid);
            if (lockDependencyGraph.hasDeadLock(tid)) {
                throw new TransactionAbortedException();
            }
            Thread.yield();
            success = lockManager.acquireLock(tid, pid,
                                              perm == Permissions.READ_ONLY ? LockType.SHARED : LockType.EXCLUSIVE);
        }
        lockDependencyGraph.clearEdge(tid);

        Page page = pageMap.get(pid);
        if (page == null) {
            while (pageMap.size() >= numPages) {
                evictPage();
            }
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageMap.put(pid, page);
            LFUCount.put(pid, new HitRate(pid, 0, System.nanoTime()));
            page.setBeforeImage();
        }
        addHitCount(pid);
        return page;
    }
    // private static final Byte restoreLock = (byte) 0;

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in wrong behavior. Think hard about who
     * needs to call this and why, and why they can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid)
    throws IOException {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    @SuppressWarnings("unused")
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
    throws IOException {
        Set<PageId> pagesOnLock = lockManager.getTidLocks(tid);
        if (pagesOnLock == null) {
            return;
        }
        if (commit) {
            flushPages(tid);
        }
        else {
            synchronized (this) {
                for (PageId pid : pagesOnLock) {
                    Page page = pageMap.get(pid);
                    if (page != null) {
                        Set<Lock> locksOnPage = lockManager.getPageLocks(pid);
                        if (locksOnPage.iterator().next().lockType == LockType.EXCLUSIVE) {
                            pageMap.put(pid, page.getBeforeImage());
                        }
                    }
                }
            }
        }
        lockManager.releaseLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will acquire a write lock on the page the tuple
     * is added to and any other pages that are updated (Lock acquisition is not needed for lab2). May block if the
     * lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit, and adds versions of
     * any pages that have been dirtied to the cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
    throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pageArrayList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        markDirty(tid, pageArrayList);
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on the page the tuple is removed from
     * and any other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit, and adds versions of
     * any pages that have been dirtied to the cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
    throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        ArrayList<Page> pageArrayList = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        markDirty(tid, pageArrayList);
    }

    private void markDirty(TransactionId tid, ArrayList<Page> dirtyPages)
    throws DbException {
        for (Page page : dirtyPages) {
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
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes dirty data to disk so will break
     * simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages()
    throws IOException {
        for (PageId pageId : pageMap.keySet()) {
            flushPage(pageId);
        }
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery manager to ensure that the buffer pool
     * doesn't keep a rolled back page in its cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages are removed from the cache so they can be reused safely
     */
    synchronized void discardPage(PageId pid) {
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
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid)
    throws IOException {
        Page page = pageMap.get(pid);
        try {
            page.markDirty(false, null);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
        catch (Exception e) {
            throw new IOException();
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    synchronized void flushPages(TransactionId tid)
    throws IOException {
        for (Page page : pageMap.values()) {
            if (tid.equals(page.isDirty())) {
                flushPage(page.getId());
                page.setBeforeImage();
            }
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage()
    throws DbException {
        HitRate LFUPage = Collections.min(LFUCount.values());
        if (pageMap.get(LFUPage.pageId).isDirty() != null) {
            throw new DbException("Exception occurred when evicting pages: all pages are dirty.");
        }
        try {
            flushPage(LFUPage.pageId);
        }
        catch (IOException e) {
            throw new DbException(String.format(
                    "IOException occurred when flushing page %d.", LFUPage.pageId.hashCode()));
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

    public enum LockType implements Serializable {
        SHARED, EXCLUSIVE
    }

    private class HitRate implements Comparable<HitRate> {
        private final PageId pageId;
        private int hitCount;
        private long hitTime;

        HitRate(PageId pageId, int hitCount, long hitTime) {
            this.pageId = pageId;
            this.hitCount = hitCount;
            this.hitTime = hitTime;
        }

        @Override
        public int compareTo(HitRate o) {
            if (pageMap.get(pageId).isDirty() != null) {
                return Integer.compare(Integer.MAX_VALUE, o.hitCount);
            }
            if (pageMap.get(o.pageId).isDirty() != null) {
                return Integer.compare(hitCount, Integer.MAX_VALUE);
            }
            int cmp = Integer.compare(hitCount, o.hitCount);
            if (cmp == 0) {
                return Long.compare(hitTime, o.hitTime);
            }
            else {
                return cmp;
            }
        }
    }

    private class Lock implements Comparable<Lock> {
        private final TransactionId tid;
        private LockType lockType;

        Lock(TransactionId tid, LockType lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }

        void setExclusiveType() {
            this.lockType = LockType.EXCLUSIVE;
        }

        TransactionId getTid() {
            return tid;
        }

        LockType getLockType() {
            return lockType;
        }

        @Override
        public int compareTo(Lock o) {
            if (lockType == LockType.EXCLUSIVE) {
                return Integer.compare(0, 1);
            }
            else if (o.lockType == LockType.EXCLUSIVE) {
                return Integer.compare(1, 0);
            }
            else {
                return Integer.compare(tid.hashCode(), o.tid.hashCode());
            }
        }
    }

    private class LockManager {
        /**
         * <del>
         * Vector is a synchronised collection implementing List interface , while ArrayList is not synchronised. Though
         * Vector performs a little worse than CopyOnWriteArrayList since it grants locks even on reading, its advantage
         * of less memory makes it a better choice. Be careful! Vector may rise ConcurrentModificationException when
         * iterating. A lock to synchronise iterating is necessary.
         * </del>
         * <p>
         * It is found that Vector is over-dated and collapsed. If a lock is necessary for all implementations, why not
         * directly use Set?
         */
        final Map<PageId, Set<Lock>> pageLockMap = new ConcurrentHashMap<>();
        private final Map<TransactionId, Set<PageId>> tidLockMap = new ConcurrentHashMap<>();

        synchronized boolean acquireLock(TransactionId tid, PageId pid, LockType lockType) {
            // checkSize();
            if (!pageLockMap.containsKey(pid)) {
                // No lock on the page, simply accept the lock.
                Lock lock = new Lock(tid, lockType);
                pageLockMap.put(pid, new HashSet<>(Collections.singleton(lock)));
                if (!tidLockMap.containsKey(tid)) {
                    tidLockMap.put(tid, new HashSet<>());
                }
                tidLockMap.get(tid).add(pid);
                return true;
            }

            Set<Lock> lockSet = pageLockMap.get(pid);
            assert !lockSet.isEmpty();
            Lock firstLock = lockSet.iterator().next();
            boolean isSameTid = firstLock.getTid().equals(tid);
            boolean flag;
            if (lockType == LockType.SHARED) {
                // SHARED lock
                if (firstLock.getLockType() == LockType.EXCLUSIVE) {
                    // an EXCLUSIVE lock already exists
                    assert lockSet.size() == 1;
                    flag = isSameTid;
                }
                else {
                    if (tidLockMap.containsKey(tid) && tidLockMap.get(tid).contains(pid)) {
                        // a SHARED lock already exists, and the same transaction requires
                        // a SHARED lock
                        return true;
                    }
                    lockSet.add(new Lock(tid, LockType.SHARED));
                    flag = true;
                }
            }
            else {
                // EXCLUSIVE lock
                if (firstLock.getLockType() == LockType.EXCLUSIVE) {
                    // an EXCLUSIVE lock already exists
                    assert lockSet.size() == 1;
                    flag = isSameTid;
                }
                else if (lockSet.size() == 1) {
                    // only one SHARED lock
                    if (isSameTid) {
                        // upgrade the lock
                        firstLock.setExclusiveType();
                        flag = true;
                    }
                    else {
                        flag = false;
                    }
                }
                else {
                    // require EXCLUSIVE lock but there are multi shared locks
                    flag = false;
                }
            }
            if (flag) {
                if (!tidLockMap.containsKey(tid)) {
                    tidLockMap.put(tid, new HashSet<>());
                }
                tidLockMap.get(tid).add(pid);
            }
            // checkSize();
            return flag;
        }

        synchronized void releaseLock(TransactionId tid, PageId pid) {
            assert pageLockMap.containsKey(pid);
            assert tidLockMap.get(tid).contains(pid);
            Set<Lock> lockSet = pageLockMap.get(pid);
            Iterator<Lock> iterator = lockSet.iterator();
            while (iterator.hasNext()) {
                Lock lock = iterator.next();
                if (lock.getTid().equals(tid)) {
                    break;
                }
            }
            iterator.remove();
            tidLockMap.get(tid).remove(pid);
            if (lockSet.isEmpty()) {
                pageLockMap.remove(pid);
            }
            if (tidLockMap.get(tid).isEmpty()) {
                tidLockMap.remove(tid);
            }
        }

        synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            if (!tidLockMap.containsKey(tid)) {
                return false;
            }
            else {
                return tidLockMap.get(tid).contains(pid);
            }
        }

        synchronized void releaseLocks(TransactionId tid) {
            for (PageId pid : pageLockMap.keySet()) {
                if (holdsLock(tid, pid)) {
                    releaseLock(tid, pid);
                }
            }
        }

        synchronized Set<Lock> getPageLocks(PageId pid) {
            if (!pageLockMap.containsKey(pid)) {
                return new HashSet<>();
            }
            else {
                return new HashSet<>(pageLockMap.get(pid));
            }
        }

        Set<PageId> getTidLocks(TransactionId tid) {
            return tidLockMap.get(tid);
        }

        @SuppressWarnings({"unused", "Duplicates"})
        private synchronized void checkSize() {
            // only for debug
            int size1 = 0;
            for (Set<Lock> lockSet : pageLockMap.values()) {
                assert lockSet.size() <= tidLockMap.size();
                size1 += lockSet.size();
            }
            int size2 = 0;
            for (Set<PageId> pageIdSet : tidLockMap.values()) {
                assert pageIdSet.size() <= pageLockMap.size();
                size2 += pageIdSet.size();
            }
            assert size1 == size2;
        }
    }

    private class LockDependencyGraph {
        private final Map<TransactionId, Set<TransactionId>> dependencyGraph = new ConcurrentHashMap<>();

        synchronized void addEdge(TransactionId tid, PageId pid) {
            if (!dependencyGraph.containsKey(tid)) {
                dependencyGraph.put(tid, new HashSet<>());
            }
            Set<TransactionId> tidSet = dependencyGraph.get(tid);
            // Use clear() instead of putting a new HashSet to save memory.
            tidSet.clear();
            Set<Lock> lockSet = lockManager.getPageLocks(pid);
            Set<TransactionId> tmp = new HashSet<>();
            synchronized (lockManager.getPageLocks(pid)) {
                for (Lock lock : lockSet) {
                    tmp.add(lock.getTid());
                }
            }
            tidSet.addAll(tmp);
        }

        synchronized boolean hasDeadLock(TransactionId tid) {
            Queue<TransactionId> queue = new LinkedList<>();
            Set<TransactionId> visited = new HashSet<>();
            queue.add(tid);
            // Transaction tid is not added to the graph now,
            // thus we need not add tid to visited.
            while (!queue.isEmpty()) {
                TransactionId now = queue.poll();
                Set<TransactionId> adj = dependencyGraph.get(now);
                if (adj == null) {
                    continue;
                }
                for (TransactionId trid : adj) {
                    if (trid.equals(tid)) {
                        return true;
                    }
                    if (!visited.contains(trid)) {
                        visited.add(trid);
                        queue.add(trid);
                    }
                }
            }
            return false;
        }

        synchronized void clearEdge(TransactionId tid) {
            if (!dependencyGraph.containsKey(tid)) {
                dependencyGraph.put(tid, new HashSet<>());
            }
            else {
                dependencyGraph.get(tid).clear();
            }
        }
    }
}
