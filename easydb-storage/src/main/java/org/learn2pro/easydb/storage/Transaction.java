package org.learn2pro.easydb.storage;

import java.io.IOException;

/**
 * Transaction encapsulates information about the state of a transaction and manages transaction commit / abort.
 */

public class Transaction {

    private final TransactionId tid;
    volatile boolean started = false;

    public Transaction() {
        tid = new TransactionId();
    }

    /**
     * Start the transaction running
     */
    public void start() {
        started = true;
        try {
            Database.getLogFile().logXactionBegin(tid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TransactionId getId() {
        return tid;
    }

    /**
     * Finish the transaction
     */
    public void commit() throws IOException,TransactionAbortedException {
        transactionComplete(false);
    }

    /**
     * Finish the transaction
     */
    public void abort() throws IOException,TransactionAbortedException {
        transactionComplete(true);
    }

    /**
     * Handle the details of transaction commit / abort
     */
    public void transactionComplete(boolean abort) throws IOException,TransactionAbortedException {

        if (started) {
            //write commit / abort records
            if (abort) {
                Database.getLogFile().logAbort(tid); //does rollback too
            } else {
                //write all the dirty pages for this transaction out
                Database.getBufferPool().flushPages(tid);
                Database.getLogFile().logCommit(tid);
            }

            Database.getBufferPool().transactionComplete(tid, !abort); // release locks

            //setting this here means we could possibly write multiple abort records -- OK?
            started = false;
        }
    }
}
