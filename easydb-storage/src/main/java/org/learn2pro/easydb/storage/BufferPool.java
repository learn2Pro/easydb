package org.learn2pro.easydb.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 * BufferPool manages the reading and writing of pages into memory from disk. Access methods call into it to retrieve
 * pages, and it fetches pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches a page, BufferPool checks that the
 * transaction has the appropriate locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by other classes. BufferPool should use the
     * numPages argument to the constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    /**
     * page number
     */
    private int numPages;
    /**
     * page data hold by memory
     */
    private LRUCache<PageId, Page> pageData;
    private PageLock pageLock;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageData = new LRUCache<>(numPages);
        this.pageLock = new PageLock();
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
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire a lock and may block if that lock is
     * held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it is present, it should be returned.  If it is
     * not present, it should be added to the buffer pool and returned.  If there is insufficient space in the buffer
     * pool, a page should be evicted and the new page should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        try {
            pageLock.lockPage(tid, pid, perm);
            Page page = pageData.get(pid);
            if (page == null) {
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                page = dbFile.readPage(pid);
                if (page != null) {
                    if (pageData.size() > this.numPages) {
                        evictPage();
                    }
                    if (perm == Permissions.READ_WRITE) {
                        page.markDirty(true, tid);
                    }
                    pageData.put(pid, page);
                }
            }
            return page;
        } finally {
//            releasePage(tid, pid);
        }
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in wrong behavior. Think hard about who
     * needs to call this and why, and why they can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        pageLock.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        pageLock.releaseLockTrans(tid);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return pageLock.readLockable(tid, p) || pageLock.writeLockable(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            //write dirty page to disk
            List<Entry<PageId, Page>> entries = new ArrayList<>(pageData.entrySet());
            for (Entry<PageId, Page> item : entries) {
                if (commit) {
                    flushPage(item.getKey());
                } else {
                    discardPage(item.getKey());
                }
            }
        } finally {
            pageLock.releaseLockTrans(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will acquire a write lock on the page the tuple
     * is added to and any other pages that are updated (Lock acquisition is not needed for lab2). May block if the
     * lock(s) cannot be acquired.
     *
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
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPage = dbFile.insertTuple(tid, t);
        for (Page page : dirtyPage) {
            page.markDirty(true, tid);
            pageData.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on the page the tuple is removed from
     * and any other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit, and adds versions of
     * any pages that have been dirtied to the cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        dbFile.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes dirty data to disk so will break
     * simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        List<Entry<PageId, Page>> entrySet = new ArrayList<>(pageData.entrySet());
        for (Entry<PageId, Page> entry : entrySet) {
            flushPage(entry.getKey());
        }
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery manager to ensure that the buffer pool
     * doesn't keep a rolled back page in its cache.
     *
     * Also used by B+ tree files to ensure that deleted pages are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageData.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = pageData.get(pid);
        if (p != null && p.isDirty() != null) {
            int tableId = p.getId().getTableId();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
            dbFile.writePage(p);
            releasePage(p.isDirty(), pid);
            p.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // clone for avoid concurrent modified exception
        List<Entry<PageId, Page>> entrySet = new ArrayList<>(pageData.entrySet());
        for (Entry<PageId, Page> entry : entrySet) {
            PageId pageId = entry.getKey();
            try {
                pageLock.lockPage(tid, pageId, Permissions.READ_WRITE);
                TransactionId hold = entry.getValue().isDirty();
                if (hold != null && hold.equals(tid)) {
                    flushPage(pageId);
                }
            } catch (TransactionAbortedException e) {
                throw new IOException(e);
            } finally {
                pageLock.releaseLock(tid, pageId);
            }
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
//        PageId pid = pageData.evictNode();
//        Page p = pageData.get(pid);
//        if (pid != null) {
//            try {
//                flushPage(pid);
//                discardPage(pid);
//            } catch (IOException e) {
//                throw new DbException(e);
//            }
//        }
        List<Entry<PageId, Page>> entrySet = new ArrayList<>(pageData.entrySet());
        for (Entry<PageId, Page> entry : entrySet) {
            if (entry.getValue().isDirty() == null) {
//                int tableId = entry.getKey().getTableId();
//                DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
//                    dbFile.writePage(entry.getValue());
                discardPage(entry.getKey());
                return;
            }
        }
        throw new DbException("Can not found clean page to evict!");
    }

}
