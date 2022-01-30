package org.learn2pro.easydb.storage;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class PageLock {

    public static final int MAX_LOCK_NUM = 128;
    /**
     * 100ms
     */
    public static final int DEFAULT_ACQUIRE_LOCK_TIMEOUT = 100;
    private TransactionId[] transactionIds;
    private PageId[] pageIds;
    private int[] lockLevel;
    private ReentrantLock lock = new ReentrantLock();

    public PageLock() {
        transactionIds = new TransactionId[MAX_LOCK_NUM];
        pageIds = new PageId[MAX_LOCK_NUM];
        lockLevel = new int[MAX_LOCK_NUM];
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            lockLevel[i] = -1;
        }
    }

    public boolean readLockable(TransactionId tid, PageId pid) {
        //this page is not readLockable when writeLocked by other transaction
        for (int itx = 0; itx < MAX_LOCK_NUM; itx++) {
            if (pageIds[itx] != null && pageIds[itx].equals(pid)) {
                if (lockLevel[itx] == 1 && !transactionIds[itx].equals(tid)) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean writeLockable(TransactionId tid, PageId pid) {
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            if (pageIds[i] != null && pageIds[i].equals(pid)) {
                if (!transactionIds[i].equals(tid)) {
                    return false;
                }
            }
        }
        return true;
    }

    public void lockPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        lockPage(tid, pid, perm, DEFAULT_ACQUIRE_LOCK_TIMEOUT);
    }

    public void lockPage(TransactionId tid, PageId pid, Permissions perm, Integer timeout)
            throws TransactionAbortedException {
        long ts = System.currentTimeMillis();
        long deadline = ts + timeout;
        for (; ts < deadline; ts = System.currentTimeMillis()) {
            try {
                if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                    if (perm == Permissions.READ_ONLY && readLockable(tid, pid)) {
                        readLock(tid, pid);
                        return;
                    } else if (perm == Permissions.READ_WRITE && writeLockable(tid, pid)) {
                        writeLock(tid, pid);
                        return;
                    } else {
                        Thread.sleep(timeout / 10);
                    }
                }
            } catch (InterruptedException e) {
                throw new TransactionAbortedException(e);
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
        throw new TransactionAbortedException("get lock timeout:" + timeout + "ms");
    }

    private void readLock(TransactionId tid, PageId pid) {
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            if (pageIds[i] == null) {
                pageIds[i] = pid;
                transactionIds[i] = tid;
                lockLevel[i] = Permissions.READ_ONLY.getPermLevel();
                return;
            }
        }
        throw new IllegalStateException("lock poll is full");
    }

    private void writeLock(TransactionId tid, PageId pid) {
        //use old lock
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            if (pageIds[i] != null && pageIds[i].equals(pid) && transactionIds[i].equals(tid)) {
                return;
            }
        }
        //create new lock
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            if (pageIds[i] == null) {
                pageIds[i] = pid;
                transactionIds[i] = tid;
                lockLevel[i] = Permissions.READ_WRITE.getPermLevel();
                return;
            }
        }
        throw new IllegalStateException("lock poll is full");
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        lock.lock();
        try {
            for (int i = 0; i < MAX_LOCK_NUM; i++) {
                if (pageIds[i] != null && pageIds[i].equals(pid) && transactionIds[i].equals(tid)) {
                    pageIds[i] = null;
                    transactionIds[i] = null;
                    lockLevel[i] = -1;
                    return;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void releaseLockTrans(TransactionId tid) {
        lock.lock();
        try {
            for (int i = 0; i < MAX_LOCK_NUM; i++) {
                if (transactionIds[i] != null && transactionIds[i].equals(tid)) {
                    pageIds[i] = null;
                    transactionIds[i] = null;
                    lockLevel[i] = -1;
                }
            }
        } finally {
            lock.unlock();
        }
    }

//    public boolean writeLocked(PageId pid) {
//        int itx;
//        for (itx = 0; itx < maxlocknum; itx++) {
//            if (Pid[itx] != null && Pid[itx].equals(pid) && ll[itx] == 2) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    public int lockLevel(TransactionId tid, PageId pid) {
//        int itx;
//        for (itx = 0; itx < maxlocknum; itx++) {
//            if (Tid[itx] != null) {
//                if (Tid[itx].equals(tid) && Pid[itx].equals(pid)) {
//                    return ll[itx];
//                }
//            }
//        }
//        return -1;
//    }
//
//    public int lockLoc(TransactionId tid, PageId pid) {
//        int itx;
//        for (itx = 0; itx < maxlocknum; itx++) {
//            if (Tid[itx] != null) {
//                if (Tid[itx].equals(tid) && Pid[itx].equals(pid)) {
//                    return itx;
//                }
//            }
//        }
//        return -1;
//    }

}
