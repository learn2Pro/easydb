package org.learn2pro.easydb.storage;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class PageLock {

    public static final int MAX_LOCK_NUM = 1024;
    /**
     * 100ms
     */
    public static final int DEFAULT_ACQUIRE_LOCK_TIMEOUT = 200;
    private TransactionId[] transactionIds;
    private PageId[] pageIds;
    private int[] lockLevel;
    private int[] cycleGraph;
    private ReentrantLock lock = new ReentrantLock();

    public PageLock() {
        transactionIds = new TransactionId[MAX_LOCK_NUM];
        pageIds = new PageId[MAX_LOCK_NUM];
        lockLevel = new int[MAX_LOCK_NUM];
        cycleGraph = new int[MAX_LOCK_NUM];
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            lockLevel[i] = -1;
            cycleGraph[i] = -1;
        }
    }

    public boolean readLockable(TransactionId tid, PageId pid) {
        //this page is not readLockable when writeLocked by other transaction
        for (int itx = 0; itx < MAX_LOCK_NUM; itx++) {
            // write by other tid
            if (pageIds[itx] != null && pageIds[itx].equals(pid) && lockLevel[itx] == 1 && !transactionIds[itx].equals(
                    tid)) {
                return false;
            }
        }
        return true;
    }

    public boolean writeLockable(TransactionId tid, PageId pid) {
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            //read/write by other tid
            if (pageIds[i] != null && pageIds[i].equals(pid) && !transactionIds[i].equals(tid)) {
                return false;
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
                        lookFor(tid, pid);
                        if (cycleCheck()) {
                            revertLookFor(tid);
                            throw new TransactionAbortedException(
                                    String.format("meet deadlock for page:%s,transaction:%s,permission:%s",
                                            pid.getPageNumber(), tid.getId(), perm));
                        }
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
        throw new TransactionAbortedException(
                "get lock timeout:" + timeout + "ms,pid:" + pid.getPageNumber() + ",permission:" + perm);
    }

    private void readLock(TransactionId tid, PageId pid) {
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            //use old lock
            if (pageIds[i] != null && pageIds[i].equals(pid) && transactionIds[i].equals(tid)) {
                lockLevel[i] = Permissions.READ_ONLY.getPermLevel();
                return;
            }
            //create new lock
            else if (pageIds[i] == null) {
                pageIds[i] = pid;
                transactionIds[i] = tid;
                lockLevel[i] = Permissions.READ_ONLY.getPermLevel();
                return;
            }
        }
        throw new IllegalStateException("lock poll is full");
    }

    private void writeLock(TransactionId tid, PageId pid) {
        //create new lock
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            //use old lock
            if (pageIds[i] != null && pageIds[i].equals(pid) && transactionIds[i].equals(tid)) {
                lockLevel[i] = Permissions.READ_WRITE.getPermLevel();
                return;
            }
            //create new lock
            else if (pageIds[i] == null) {
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
                    releaseByForward(i);
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
                    releaseByForward(i);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public Set<PageId> getPagesHeldByTid(TransactionId tid) {
        Set<PageId> pageSet = Sets.newHashSet();
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            if (transactionIds[i] != null && transactionIds[i].equals(tid) && pageIds[i] != null) {
                pageSet.add(pageIds[i]);
            }
        }
        return pageSet;
    }

    private boolean cycleCheck() {
        int p0 = 0, p1 = 0;
        int step = 0;
        while (p0 < MAX_LOCK_NUM && p1 < MAX_LOCK_NUM) {
            if (p0 == p1 && cycleGraph[p0] == cycleGraph[p1] && step != 0) {
                return true;
            }
            p0 = step(p0, 1);
            p1 = step(p1, 2);
            step += 1;
        }
        return false;
    }

    private int step(int current, int gap) {
        for (int i = gap; i > 0 && current < MAX_LOCK_NUM; i--) {
            if (cycleGraph[current] == -1) {
                current += 1;
            } else {
                current = cycleGraph[current];
            }
        }
        return current;
    }

    private void lookFor(TransactionId tid, PageId pid) {
        Set<Integer> hold = Sets.newHashSet();
        List<Integer> forward = Lists.newArrayList();
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            if (transactionIds[i] != null && transactionIds[i].equals(tid)) {
                hold.add(i);
            }
        }
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            if (pageIds[i] != null && pageIds[i].equals(pid) && !hold.contains(i)) {
                forward.add(i);
            }
        }
        if (!hold.isEmpty() && !forward.isEmpty()) {
            for (Integer idx : hold) {
                cycleGraph[idx] = forward.get(0);
            }
        }
    }

    private void revertLookFor(TransactionId tid) {
        List<Integer> hold = Lists.newArrayList();
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            if (transactionIds[i] != null && transactionIds[i].equals(tid)) {
                hold.add(i);
            }
        }
        if (!hold.isEmpty()) {
            for (Integer idx : hold) {
                cycleGraph[idx] = -1;
            }
        }
    }

    private void releaseByForward(int idx) {
        for (int i = 0; i < MAX_LOCK_NUM; i++) {
            if (cycleGraph[i] == idx) {
                cycleGraph[i] = -1;
            }
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
